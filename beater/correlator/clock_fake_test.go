package correlator

import (
	"sync"
	"time"
)

// fakeClock is a manually-advanced Clock for deterministic tests: Now()
// never moves on its own, and NewTicker's channel only fires when the test
// calls Advance.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTicker(time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTicker{c: make(chan time.Time, 1)}
	c.tickers = append(c.tickers, t)
	return t
}

// Advance moves the fake clock forward by d and fires every outstanding,
// non-stopped ticker once with the new time. The send is non-blocking (the
// channel is buffered) so Advance itself never blocks; the engine's actor
// goroutine picks the tick up on its own schedule.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	tickers := append([]*fakeTicker(nil), c.tickers...)
	c.mu.Unlock()

	for _, t := range tickers {
		if t.stopped() {
			continue
		}
		select {
		case t.c <- now:
		default:
		}
	}
}

type fakeTicker struct {
	c chan time.Time

	mu   sync.Mutex
	done bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.c }

func (t *fakeTicker) Stop() {
	t.mu.Lock()
	t.done = true
	t.mu.Unlock()
}

func (t *fakeTicker) stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}
