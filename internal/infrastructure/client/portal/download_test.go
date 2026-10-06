package portal

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

func TestWindowLimiterAllowsLimitPerWindow(t *testing.T) {
	const window = 300 * time.Millisecond
	l := newWindowLimiter(3, window)

	start := time.Now()
	for range 3 {
		l.Wait()
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("first 3 calls should not wait, took %s", elapsed)
	}

	l.Wait()
	if elapsed := time.Since(start); elapsed < window {
		t.Fatalf("4th call should wait for the window to slide (%s), took %s", window, elapsed)
	}
}

func TestWindowLimiterIsSharedAcrossGoroutines(t *testing.T) {
	const window = 200 * time.Millisecond
	l := newWindowLimiter(5, window)

	var immediate int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Wait() < window/2 {
				atomic.AddInt32(&immediate, 1)
			}
		}()
	}
	wg.Wait()

	if immediate != 5 {
		t.Fatalf("expected exactly 5 of 10 concurrent calls to pass without waiting, got %d", immediate)
	}
}

func TestWindowLimiterBlock(t *testing.T) {
	l := newWindowLimiter(10, time.Minute)
	l.Block(200 * time.Millisecond)

	if waited := l.Wait(); waited < 200*time.Millisecond {
		t.Fatalf("Wait should hold during a block, waited %s", waited)
	}
}

// portalServer answers the first blockedRequests requests (all, if negative)
// the way the portal's WAF does, and the rest with status and a ZIP-like body.
func portalServer(t *testing.T, blockedRequests int, status int) (*httptest.Server, *int32) {
	t.Helper()
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requests, 1)
		if blockedRequests < 0 || int(n) <= blockedRequests {
			w.Header().Set(wafActionHeader, "captcha")
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("<html>Human Verification</html>"))
			return
		}
		w.WriteHeader(status)
		w.Write([]byte("PK-zip-content"))
	}))
	t.Cleanup(srv.Close)
	return srv, &requests
}

func newTestClient(srv *httptest.Server, window time.Duration) *transparencyPortalClient {
	opts := DownloadOptions{Limit: 100, Window: window}
	return &transparencyPortalClient{
		logger:       &logger.Logger{MinLevel: logger.LevelError + 1},
		baseUrl:      srv.URL + "/",
		client:       srv.Client(),
		downloadOpts: opts,
		limiter:      newWindowLimiter(opts.Limit, opts.Window),
	}
}

func TestDownloadSuccess(t *testing.T) {
	srv, requests := portalServer(t, 0, http.StatusOK)
	c := newTestClient(srv, time.Minute)
	out := filepath.Join(t.TempDir(), "file.zip")

	res := c.download("date=20250101", srv.URL, out)

	if !res.Success || res.OutputPath != out {
		t.Fatalf("expected success with path %s, got %+v", out, res)
	}
	if got, _ := os.ReadFile(out); string(got) != "PK-zip-content" {
		t.Errorf("unexpected file content %q", got)
	}
	if _, err := os.Stat(out + ".part"); !os.IsNotExist(err) {
		t.Errorf("temporary .part file should be gone")
	}
	if *requests != 1 {
		t.Errorf("expected 1 request, got %d", *requests)
	}
}

func TestDownloadRetriesAfterRateLimit(t *testing.T) {
	const window = 100 * time.Millisecond
	srv, requests := portalServer(t, 1, http.StatusOK)
	c := newTestClient(srv, window)
	out := filepath.Join(t.TempDir(), "file.zip")

	start := time.Now()
	res := c.download("date=20250101", srv.URL, out)

	if !res.Success {
		t.Fatalf("expected success after the block, got %+v", res)
	}
	if *requests != 2 {
		t.Errorf("expected 2 requests (blocked + retry), got %d", *requests)
	}
	if elapsed := time.Since(start); elapsed < window {
		t.Errorf("retry should wait for the pause (%s), took %s", window, elapsed)
	}
}

func TestDownloadGivesUpWhenStillRateLimited(t *testing.T) {
	srv, requests := portalServer(t, -1, http.StatusOK)
	c := newTestClient(srv, 10*time.Millisecond)
	out := filepath.Join(t.TempDir(), "file.zip")

	res := c.download("date=20250101", srv.URL, out)

	if res.Success {
		t.Fatal("expected failure while permanently blocked")
	}
	if want := int32(maxBlockedRetries + 1); *requests != want {
		t.Errorf("expected %d requests, got %d", want, *requests)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("no file should be written for a blocked download")
	}
}

func TestDownloadDoesNotRetryOtherErrors(t *testing.T) {
	srv, requests := portalServer(t, 0, http.StatusNotFound)
	c := newTestClient(srv, time.Minute)
	out := filepath.Join(t.TempDir(), "file.zip")

	res := c.download("date=20250101", srv.URL, out)

	if res.Success {
		t.Fatal("expected failure for a 404")
	}
	if *requests != 1 {
		t.Errorf("a 404 must not be retried, got %d requests", *requests)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("no file should be written for a 404")
	}
}
