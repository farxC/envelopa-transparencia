package portal

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

func TestPacerSpacesDownloads(t *testing.T) {
	const interval = 100 * time.Millisecond
	p := newPacer(interval, time.Minute)

	start := time.Now()
	if waited := p.Wait(); waited > 20*time.Millisecond {
		t.Fatalf("first download should start immediately, waited %s", waited)
	}
	p.Wait()
	p.Wait()
	if elapsed := time.Since(start); elapsed < 2*interval {
		t.Fatalf("3 downloads should take at least 2 intervals (%s), took %s", 2*interval, elapsed)
	}
}

func TestPacerIsSharedAcrossGoroutines(t *testing.T) {
	const interval = 50 * time.Millisecond
	p := newPacer(interval, time.Minute)

	var mu sync.Mutex
	var starts []time.Time
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Wait()
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()

	slices.SortFunc(starts, func(a, b time.Time) int { return a.Compare(b) })
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < interval-5*time.Millisecond {
			t.Errorf("starts %d and %d are only %s apart, want at least %s", i-1, i, gap, interval)
		}
	}
}

func TestPacerBlockPausesAndDoublesInterval(t *testing.T) {
	const interval, pause = 50 * time.Millisecond, 200 * time.Millisecond
	p := newPacer(interval, pause)
	p.Wait()

	newInterval, started := p.Block()
	if !started || newInterval != 2*interval {
		t.Fatalf("Block() = (%s, %t), want (%s, true)", newInterval, started, 2*interval)
	}
	if again, started := p.Block(); started || again != 2*interval {
		t.Fatalf("second Block() during the pause = (%s, %t), want (%s, false)", again, started, 2*interval)
	}

	if waited := p.Wait(); waited < pause-10*time.Millisecond {
		t.Fatalf("Wait should hold for the pause (%s), waited %s", pause, waited)
	}
	if waited := p.Wait(); waited < 2*interval-10*time.Millisecond {
		t.Fatalf("after a block downloads should be %s apart, waited %s", 2*interval, waited)
	}
}

func TestPacerIntervalIsCapped(t *testing.T) {
	p := newPacer(maxInterval, 0)
	if interval, _ := p.Block(); interval != maxInterval {
		t.Fatalf("interval should be capped at %s, got %s", maxInterval, interval)
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

func newTestClient(srv *httptest.Server, pause time.Duration) *transparencyPortalClient {
	return &transparencyPortalClient{
		logger:  &logger.Logger{MinLevel: logger.LevelError + 1},
		baseUrl: srv.URL + "/",
		client:  srv.Client(),
		pacer:   newPacer(time.Millisecond, pause),
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
	const pause = 100 * time.Millisecond
	srv, requests := portalServer(t, 1, http.StatusOK)
	c := newTestClient(srv, pause)
	out := filepath.Join(t.TempDir(), "file.zip")

	start := time.Now()
	res := c.download("date=20250101", srv.URL, out)

	if !res.Success {
		t.Fatalf("expected success after the block, got %+v", res)
	}
	if *requests != 2 {
		t.Errorf("expected 2 requests (blocked + retry), got %d", *requests)
	}
	if elapsed := time.Since(start); elapsed < pause {
		t.Errorf("retry should wait for the pause (%s), took %s", pause, elapsed)
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
