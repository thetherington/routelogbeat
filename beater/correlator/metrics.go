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

	unresolvedNoSlabMatch       atomic.Int64
	unresolvedMulticastConflict atomic.Int64
	droppedNoSlabMatch          atomic.Int64
	droppedMulticastConflict    atomic.Int64

	filteredNoDstMetadata atomic.Int64
}

// countUnresolved records a slab/magrtrsrv record's first miss, in the total
// and under its reason.
func (s *statsCounters) countUnresolved(reason MissReason) {
	s.unresolvedSlab.Add(1)
	switch reason {
	case MissNoSlabMatch:
		s.unresolvedNoSlabMatch.Add(1)
	case MissMulticastConflict:
		s.unresolvedMulticastConflict.Add(1)
	}
}

// countDropped records a pending record being discarded, in the total and
// under the reason it still had at that point.
func (s *statsCounters) countDropped(reason MissReason) {
	s.pendingDropped.Add(1)
	switch reason {
	case MissNoSlabMatch:
		s.droppedNoSlabMatch.Add(1)
	case MissMulticastConflict:
		s.droppedMulticastConflict.Add(1)
	}
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

		UnresolvedNoSlabMatch:       s.unresolvedNoSlabMatch.Load(),
		UnresolvedMulticastConflict: s.unresolvedMulticastConflict.Load(),
		DroppedNoSlabMatch:          s.droppedNoSlabMatch.Load(),
		DroppedMulticastConflict:    s.droppedMulticastConflict.Load(),

		FilteredNoDstMetadata: s.filteredNoDstMetadata.Load(),
	}
}
