package correlator

import "time"

// Ticker is the subset of *time.Ticker the engine needs, abstracted so tests
// can fire ticks by hand instead of waiting on a real timer.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Clock is the engine's injected time source. Every time-based decision
// (close_after, the sweep_interval ticker, pending_wait expiry) goes through
// this instead of calling time.Now()/time.NewTicker() directly, so tests can
// advance time deterministically.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
}

// RealClock is the production Clock: it delegates straight to package time.
// This is what WithClock(correlator.RealClock{}) wires in.
type RealClock struct{}

// Now returns time.Now().
func (RealClock) Now() time.Time { return time.Now() }

// NewTicker wraps time.NewTicker.
func (RealClock) NewTicker(d time.Duration) Ticker {
	return realTicker{time.NewTicker(d)}
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }
