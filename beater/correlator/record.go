package correlator

import "time"

// Kind identifies which log format a Record was parsed from, and — for
// magnum — which of its two variants matched.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindScheduler
	KindMagnumSubscribe // magnum "Subscribe request."
	KindMagnumComplete  // magnum "Subscription Request Complete:"
	KindSlab
	KindMagrtrsrv // magrtrsrv "set.rx.route" command
)

// String returns the fine-grained name used by ParseKind and the
// correlator.open_on config value: "scheduler", "magnum_subscribe",
// "magnum_complete", "slab", or "magrtrsrv".
func (k Kind) String() string {
	switch k {
	case KindScheduler:
		return "scheduler"
	case KindMagnumSubscribe:
		return "magnum_subscribe"
	case KindMagnumComplete:
		return "magnum_complete"
	case KindSlab:
		return "slab"
	case KindMagrtrsrv:
		return "magrtrsrv"
	default:
		return "unknown"
	}
}

// Family returns the coarse parser label shared by all variants of a format:
// "scheduler", "magnum", "slab", or "magrtrsrv".
func (k Kind) Family() string {
	switch k {
	case KindScheduler:
		return "scheduler"
	case KindMagnumSubscribe, KindMagnumComplete:
		return "magnum"
	case KindSlab:
		return "slab"
	case KindMagrtrsrv:
		return "magrtrsrv"
	default:
		return "unknown"
	}
}

// ParseKind parses a Kind.String() value, as used by the beater to interpret
// the correlator.open_on config setting.
func ParseKind(s string) (Kind, bool) {
	switch s {
	case "scheduler":
		return KindScheduler, true
	case "magnum_subscribe":
		return KindMagnumSubscribe, true
	case "magnum_complete":
		return KindMagnumComplete, true
	case "slab":
		return KindSlab, true
	case "magrtrsrv":
		return KindMagrtrsrv, true
	default:
		return KindUnknown, false
	}
}

// RawLog is the normalized unit the engine ingests. There is no type tag —
// the engine identifies (or discards) each line by matching its content
// against the registered parsers. The beater builds this from a
// SyslogMessage (see beater/rawlog.go).
type RawLog struct {
	Line     string    // the bare log text, syslog prefix already stripped
	Time     time.Time // when the device generated the log — authoritative for measurement
	Hostname string    // the emitting device's name, when present (structured, not parsed from Line)
}

// Key is the correlation identity: a (source, destination) UUID pair,
// lower-cased.
type Key struct{ Src, Dst string }

// SlabObs is a slab or magrtrsrv observation extracted from a log line: the
// hostname+number pair used to resolve the record's Key, plus a multicast
// address carried as enrichment only — never used to resolve anything.
type SlabObs struct {
	Hostname  string
	DstNum    int
	Multicast string
}

// Record is one correlated log line.
type Record struct {
	Kind   Kind
	Source string // coarse parser label: "scheduler" | "magnum" | "slab" | "magrtrsrv"
	Time   time.Time
	Key    *Key           // nil for slab/magrtrsrv until matched against an open envelope
	Slab   *SlabObs       // set for slab and magrtrsrv only
	Fields map[string]any // parser extras: variant, routes token, in-line ts, udp_port, source_ips
	Raw    string         // the original RawLog.Line
}

// CloseReason records why an envelope was dispatched.
type CloseReason string

const (
	ReasonCloseAfter CloseReason = "close_after" // fixed lifetime elapsed
	ReasonSuperseded CloseReason = "superseded"  // a newer route to the same Dst (different Src) opened
	ReasonMaxOpen    CloseReason = "max_open"    // evicted to make room under MaxOpen
	ReasonShutdown   CloseReason = "shutdown"    // engine Close()d with this envelope still open
)

// ResolvedAttrs is envelope-side enrichment assembled from the two
// independent, optional resolvers plus the records themselves — never
// returned as a single unit by either resolver. See "The lookup map" and
// "Metadata resolver" in the design notes.
type ResolvedAttrs struct {
	Slabs     []SlabRef      // expected slab endpoints for this destination (informational)
	Multicast string         // learned from records as they attach; magrtrsrv always wins
	SrcMeta   map[string]any // MetadataResolver.Metadata(Key.Src, RoleSrc), if configured
	DstMeta   map[string]any // MetadataResolver.Metadata(Key.Dst, RoleDst), if configured
}

// Envelope groups every log correlated to one route change.
type Envelope struct {
	Key          Key
	OpenedBy     Kind      // the Kind of the record that opened it
	Partial      bool      // opened by a non-open_on record; the true opener never arrived (yet)
	OpenedAt     time.Time // timestamp of the earliest correlated log
	LastAt       time.Time // timestamp of the latest correlated log
	Records      []Record  // every correlated log, in arrival order
	Resolved     ResolvedAttrs
	Reason       CloseReason
	SupersededBy *Key // set only when Reason == ReasonSuperseded
}

// Duration is the span from the first correlated log to the last, whatever
// kinds they are.
func (e *Envelope) Duration() time.Duration { return e.LastAt.Sub(e.OpenedAt) }

// SourceCounts tallies Records by their coarse Source label, e.g.
// {"scheduler":1,"magnum":2,"slab":2}.
func (e *Envelope) SourceCounts() map[string]int {
	counts := make(map[string]int, 4)
	for _, r := range e.Records {
		counts[r.Source]++
	}
	return counts
}

// SchedulerToSlabMillis is the elapsed time, in milliseconds, from the
// scheduler record's Time to the latest slab record's Time. ok is false
// unless the envelope holds at least one KindScheduler record and at least
// one KindSlab record; magnum and magrtrsrv records are ignored entirely by
// this computation, and it does not depend on OpenedBy/Partial/Duration(): a
// partial envelope that magnum opened, with scheduler attaching later, still
// qualifies once both kinds are present. If more than one scheduler record
// is present (both V1 and V2 fired), the earliest is used.
func (e *Envelope) SchedulerToSlabMillis() (int64, bool) {
	var schedTime, lastSlab time.Time
	haveSched, haveSlab := false, false
	for _, r := range e.Records {
		switch r.Kind {
		case KindScheduler:
			if !haveSched || r.Time.Before(schedTime) {
				schedTime, haveSched = r.Time, true
			}
		case KindSlab:
			if !haveSlab || r.Time.After(lastSlab) {
				lastSlab, haveSlab = r.Time, true
			}
		}
	}
	if !haveSched || !haveSlab {
		return 0, false
	}
	return lastSlab.Sub(schedTime).Milliseconds(), true
}
