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
// AWS WAF rate-based rule. Measured on 2026-10-07 with evenly spaced requests:
//
//   - It blocks once a client sends more than ~20 requests within 5 minutes,
//     answering every request with a CAPTCHA page (405 + "x-amzn-waf-action").
//   - It reacts 25-60 seconds late, so a fast burst gets 80-100+ requests
//     through before the block. Bursts were what made the limit look like ~100.
//   - The block lifts once the 5-minute count drops back under the threshold.
//
// Downloads are therefore spaced evenly instead of sent in bursts.

const (
	userAgent         = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
	wafActionHeader   = "X-Amzn-Waf-Action"
	maxBlockedRetries = 3
	// blockPause is how long all downloads pause after a block: one portal
	// window plus a second, after which our count is back under the threshold.
	blockPause = 5*time.Minute + time.Second
	// maxInterval caps the interval after repeated blocks double it.
	maxInterval = 2 * time.Minute
)

var errRateLimited = errors.New("blocked by the portal's rate limit (CAPTCHA)")

// DownloadOptions controls how fast the client downloads from the portal.
type DownloadOptions struct {
	// Interval is the minimum time between the starts of two downloads.
	Interval time.Duration
}

// DefaultDownloadOptions starts one download every 20 seconds: 15 per 5
// minutes, 25% under the portal's threshold of ~20.
func DefaultDownloadOptions() DownloadOptions {
	return DownloadOptions{Interval: 20 * time.Second}
}

// pacer spaces download starts at least interval apart and holds every caller
// while a block (see Block) is active. It is shared by all workers so the pace
// applies to the process as a whole.
//
// The state is in memory only: it is NOT shared between processes, and a new
// process starts with no history. The portal counts requests per public IP, so
// concurrent or back-to-back ETL runs can together exceed the portal's limit
// even when each one keeps its own pace.
type pacer struct {
	mu           sync.Mutex
	interval     time.Duration
	pause        time.Duration
	next         time.Time // earliest start of the next download
	blockedUntil time.Time
}

func newPacer(interval, pause time.Duration) *pacer {
	return &pacer{interval: interval, pause: pause}
}

// Wait blocks until a download may start and returns how long it waited.
func (p *pacer) Wait() time.Duration {
	begin := time.Now()
	for {
		p.mu.Lock()
		now := time.Now()
		if !now.Before(p.next) {
			p.next = now.Add(p.interval)
			p.mu.Unlock()
			return now.Sub(begin)
		}
		delay := p.next.Sub(now)
		p.mu.Unlock()
		time.Sleep(delay)
	}
}

// Block pauses every download for the block pause and doubles the interval
// (up to maxInterval) for the rest of the run. Blocks reported while a pause is
// already active don't extend it or double the interval again. It returns the
// interval now in effect and whether this call started a new pause.
func (p *pacer) Block() (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if now.Before(p.blockedUntil) {
		return p.interval, false
	}
	p.blockedUntil = now.Add(p.pause)
	if p.blockedUntil.After(p.next) {
		p.next = p.blockedUntil
	}
	p.interval = min(p.interval*2, maxInterval)
	return p.interval, true
}

// download fetches url into outputPath, keeping downloads evenly spaced. When
// the portal blocks the request, all downloads pause, the interval doubles and
// the request is retried, up to maxBlockedRetries times. label identifies the
// file in logs.
func (c *transparencyPortalClient) download(label, url, outputPath string) service.DownloadResult {
	const component = "Downloader"

	for attempt := 0; ; attempt++ {
		waited := c.pacer.Wait()
		c.logger.Debug(component, "Starting download: %s url=%s waited=%s", label, url, waited.Round(time.Second))
		size, err := c.fetchToFile(url, outputPath)

		if errors.Is(err, errRateLimited) {
			if attempt >= maxBlockedRetries {
				c.logger.Error(component, "Download failed, still blocked by the portal's rate limit after %d retries: %s", maxBlockedRetries, label)
				return service.DownloadResult{Success: false}
			}
			if interval, started := c.pacer.Block(); started {
				c.logger.Warn(component, "Blocked by the portal's rate limit (CAPTCHA), pausing all downloads: %s pause=%s newInterval=%s retry=%d/%d",
					label, c.pacer.pause, interval, attempt+1, maxBlockedRetries)
			} else {
				c.logger.Warn(component, "Blocked by the portal's rate limit (CAPTCHA) during an active pause: %s retry=%d/%d", label, attempt+1, maxBlockedRetries)
			}
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
