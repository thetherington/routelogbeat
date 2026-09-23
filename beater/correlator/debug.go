package correlator

import (
	"sort"
	"time"
)

// MissReason says why a slab/magrtrsrv record could not be resolved to an
// open envelope.
type MissReason uint8

const (
	// MissNone means the record resolved.
	MissNone MissReason = iota
	// MissNoSlabMatch: the record's {hostname, DST#} is not in the slab list of
	// any currently-open envelope. Either no envelope is open for its
	// destination (its scheduler/magnum log never opened one, or it already
	// closed), the resolver returned no slab list for that destination, or the
	// list it returned doesn't contain this hostname/DST# pair.
	MissNoSlabMatch
	// MissMulticastConflict: {hostname, DST#} matched an open envelope, but the
	// slab record's own multicast differs from the one a magrtrsrv record
	// already reported for that envelope.
	MissMulticastConflict
)

// String returns "resolved", "no_slab_match", or "multicast_conflict".
func (r MissReason) String() string {
	switch r {
	case MissNone:
		return "resolved"
	case MissNoSlabMatch:
		return "no_slab_match"
	case MissMulticastConflict:
		return "multicast_conflict"
	default:
		return "unknown"
	}
}

// UnresolvedEvent describes a slab/magrtrsrv record the engine could not (yet)
// correlate, with enough context to work out why. It is delivered to the
// observer set via WithUnresolvedObserver: once when the record is first
// buffered (Dropped == false), and once more if it is eventually discarded
// after pending_wait (Dropped == true), where Reason is the reason at the
// moment of the drop — i.e. the final answer to "why was this never
// correlated".
type UnresolvedEvent struct {
	Dropped bool
	Reason  MissReason

	// The record itself.
	Kind       Kind
	Hostname   string
	DstNum     int
	Multicast  string // the record's own multicast ("" if its line had no ADDR)
	RecordTime time.Time
	Waited     time.Duration // time spent pending; 0 for the first (buffered) event

	// MissMulticastConflict only: the open envelope the record's hostname/DST#
	// matched, and the multicast a magrtrsrv record already reported for it.
	MatchedKey        *Key
	ExpectedMulticast string

	// MissNoSlabMatch only: what the engine does know, to narrow the cause.
	//
	// OpenEnvelopes == 0 means nothing was open to match against (slab arrived
	// with no route in flight, or after its envelope closed).
	// OpenWithoutSlabs > 0 means some open envelopes have no slab list at all,
	// i.e. the resolver returned nothing for their destination.
	// KnownDstNumsForHostname non-empty (but not containing DstNum) means the
	// hostname is known but under different DST numbers; likewise
	// KnownHostnamesForDstNum for the DST number under other hostnames. Both
	// empty means neither the hostname nor the DST number appears in any open
	// envelope's slab list.
	OpenEnvelopes           int
	OpenWithoutSlabs        int
	KnownDstNumsForHostname []int
	KnownHostnamesForDstNum []string
}

// UnresolvedObserver receives an UnresolvedEvent. It is called synchronously
// on the engine's single actor goroutine, so it must be fast and must not
// block (log it, don't do I/O that can stall).
type UnresolvedObserver func(UnresolvedEvent)

// WithUnresolvedObserver installs an observer for slab/magrtrsrv records that
// fail to resolve. Building each event walks the slab index, so only install
// one when you actually want the detail (e.g. when debug logging is on); with
// no observer the engine does none of that work and only bumps the counters.
func WithUnresolvedObserver(o UnresolvedObserver) Option {
	return func(e *Engine) { e.observer = o }
}

// notifyUnresolved builds and delivers an event for rec. Callers check
// e.observer != nil first so the diagnostics are never computed unobserved.
func (e *Engine) notifyUnresolved(rec Record, reason MissReason, dropped bool, waited time.Duration) {
	ev := UnresolvedEvent{
		Dropped:    dropped,
		Reason:     reason,
		Kind:       rec.Kind,
		Hostname:   rec.Slab.Hostname,
		DstNum:     rec.Slab.DstNum,
		Multicast:  rec.Slab.Multicast,
		RecordTime: rec.Time,
		Waited:     waited,
	}

	switch reason {
	case MissMulticastConflict:
		key := e.slabIndex[SlabRef{Hostname: rec.Slab.Hostname, DstNum: rec.Slab.DstNum}]
		ev.MatchedKey = &key
		if env, ok := e.open[key]; ok {
			ev.ExpectedMulticast = env.Resolved.Multicast
		}

	case MissNoSlabMatch:
		ev.OpenEnvelopes = len(e.open)
		for key := range e.open {
			if len(e.envSlabs[key]) == 0 {
				ev.OpenWithoutSlabs++
			}
		}
		dstNums := map[int]struct{}{}
		hostnames := map[string]struct{}{}
		for ref := range e.slabIndex {
			if ref.Hostname == rec.Slab.Hostname {
				dstNums[ref.DstNum] = struct{}{}
			}
			if ref.DstNum == rec.Slab.DstNum {
				hostnames[ref.Hostname] = struct{}{}
			}
		}
		for n := range dstNums {
			ev.KnownDstNumsForHostname = append(ev.KnownDstNumsForHostname, n)
		}
		for h := range hostnames {
			ev.KnownHostnamesForDstNum = append(ev.KnownHostnamesForDstNum, h)
		}
		sort.Ints(ev.KnownDstNumsForHostname)
		sort.Strings(ev.KnownHostnamesForDstNum)
	}

	e.observer(ev)
}
