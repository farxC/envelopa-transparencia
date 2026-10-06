package portal

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

// The portal's download host (dadosabertos-download.cgu.gov.br) sits behind an
// AWS WAF rate-based rule: after roughly 80-100 requests within its evaluation
// window (observed to be at most 5 minutes) it answers every request with a
// CAPTCHA page (405 + "x-amzn-waf-action: captcha") until the request rate drops.
// The threshold is not exact: blocks were observed after 103 and after 82
// requests sent in under 30 seconds.

const (
	userAgent          = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
	wafActionHeader    = "X-Amzn-Waf-Action"
	maxBlockedRetries  = 3
	throttleLogMinWait = time.Second
)

var errRateLimited = errors.New("blocked by the portal's rate limit (CAPTCHA)")

// DownloadOptions controls how fast the client downloads from the portal.
type DownloadOptions struct {
	// Limit is the maximum number of downloads started within any Window.
	Limit int
	// Window is the sliding window Limit applies to. It is also how long all
	// downloads pause after the portal blocks a request.
	Window time.Duration
}

// DefaultDownloadOptions stays below the lowest observed block point (82
// requests). The window is one second longer than the portal's 5 minutes: a
// pause of exactly 5 minutes ended while the portal still counted our last
// requests, and the next requests were blocked again.
func DefaultDownloadOptions() DownloadOptions {
	return DownloadOptions{Limit: 70, Window: 5*time.Minute + time.Second}
}

// windowLimiter allows at most limit calls to Wait to return within any window,
// and holds every caller while a block (see Block) is active. It is shared by
// all workers so the limit applies to the process as a whole.
//
// The state is in memory only: it is NOT shared between processes, and a new
// process starts with an empty window. The portal counts requests per public IP,
// so concurrent or back-to-back ETL runs can together exceed the portal's limit
// even when each one stays under its own.
type windowLimiter struct {
	mu           sync.Mutex
	limit        int
	window       time.Duration
	started      []time.Time // start times within the current window, oldest first
	blockedUntil time.Time
}

func newWindowLimiter(limit int, window time.Duration) *windowLimiter {
	return &windowLimiter{limit: limit, window: window}
}

// Wait blocks until a download may start and returns how long it waited.
func (l *windowLimiter) Wait() time.Duration {
	begin := time.Now()
	for {
		l.mu.Lock()
		now := time.Now()
		for len(l.started) > 0 && now.Sub(l.started[0]) >= l.window {
			l.started = l.started[1:]
		}

		var delay time.Duration
		switch {
		case now.Before(l.blockedUntil):
			delay = l.blockedUntil.Sub(now)
		case len(l.started) >= l.limit:
			delay = l.started[0].Add(l.window).Sub(now)
		default:
			l.started = append(l.started, now)
			l.mu.Unlock()
			return now.Sub(begin)
		}
		l.mu.Unlock()
		time.Sleep(delay)
	}
}

// Block makes every Wait hold for at least d from now.
func (l *windowLimiter) Block(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if until := time.Now().Add(d); until.After(l.blockedUntil) {
		l.blockedUntil = until
	}
}

// download fetches url into outputPath, respecting the download limit. When the
// portal blocks the request, all downloads pause for one window and the request
// is retried, up to maxBlockedRetries times. label identifies the file in logs.
func (c *transparencyPortalClient) download(label, url, outputPath string) service.DownloadResult {
	const component = "Downloader"

	for attempt := 0; ; attempt++ {
		if waited := c.limiter.Wait(); waited >= throttleLogMinWait {
			c.logger.Info(component, "Download throttled to stay under the portal's rate limit: %s waited=%s", label, waited.Round(time.Second))
		}

		c.logger.Debug(component, "Starting download: %s url=%s", label, url)
		size, err := c.fetchToFile(url, outputPath)

		if errors.Is(err, errRateLimited) {
			if attempt >= maxBlockedRetries {
				c.logger.Error(component, "Download failed, still blocked by the portal's rate limit after %d retries: %s", maxBlockedRetries, label)
				return service.DownloadResult{Success: false}
			}
			c.limiter.Block(c.downloadOpts.Window)
			c.logger.Warn(component, "Blocked by the portal's rate limit (CAPTCHA), pausing all downloads: %s pause=%s retry=%d/%d",
				label, c.downloadOpts.Window, attempt+1, maxBlockedRetries)
			continue
		}
		if err != nil {
			c.logger.Warn(component, "Download failed: %s error=%v", label, err)
			return service.DownloadResult{Success: false}
		}

		c.logger.Info(component, "Download completed: %s path=%s size=%d bytes", label, outputPath, size)
		return service.DownloadResult{Success: true, OutputPath: outputPath}
	}
}

// fetchToFile downloads url into outputPath. The body is written to a temporary
// file that is renamed on success, so an interrupted download never leaves a
// partial ZIP that later runs would reuse.
func (c *transparencyPortalClient) fetchToFile(url, outputPath string) (int64, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.Header.Get(wafActionHeader) != "" {
			return 0, errRateLimited
		}
		return 0, fmt.Errorf("unexpected status %s", resp.Status)
	}

	tmpPath := outputPath + ".part"
	out, err := os.Create(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("create file: %w", err)
	}
	size, err := io.Copy(out, resp.Body)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("write file: %w", err)
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename file: %w", err)
	}
	return size, nil
}
