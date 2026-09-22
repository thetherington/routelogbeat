package correlator

import "sync/atomic"

// statsCounters backs Engine.Stats(). Every field is written only from the
// actor goroutine but read from Stats() on any goroutine, hence atomic.Int64
// rather than plain int64.
type statsCounters struct {
	submitted        atomic.Int64
	discarded        atomic.Int64
	parseErrors      atomic.Int64
	unresolvedSlab   atomic.Int64
	pendingDropped   atomic.Int64
	envelopesOpened  atomic.Int64
	envelopesClosed  atomic.Int64
	maxOpenEvictions atomic.Int64
	sweeps           atomic.Int64
	ingests          atomic.Int64
}

func (s *statsCounters) snapshot() Stats {
	return Stats{
		Submitted:        s.submitted.Load(),
		Discarded:        s.discarded.Load(),
		ParseErrors:      s.parseErrors.Load(),
		UnresolvedSlab:   s.unresolvedSlab.Load(),
		PendingDropped:   s.pendingDropped.Load(),
		EnvelopesOpened:  s.envelopesOpened.Load(),
		EnvelopesClosed:  s.envelopesClosed.Load(),
		MaxOpenEvictions: s.maxOpenEvictions.Load(),
		Sweeps:           s.sweeps.Load(),
		Ingests:          s.ingests.Load(),
	}
}
