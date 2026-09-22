// Package correlator groups related route-provisioning log lines —
// scheduler, magnum, slab, and magrtrsrv, arriving in different text
// formats — into a single timed Envelope per route change, so a caller can
// measure how long the change took.
//
// A single actor goroutine (started by New, stopped by Close) owns all
// mutable state: it receives RawLog values via Submit/TrySubmit, matches
// each against a set of Parsers, correlates the resulting Records by a
// (src, dst) UUID Key, and dispatches closed *Envelope values on the
// channel returned by Events.
//
//	run():
//	  ticker = clock.NewTicker(SweepInterval)
//	  for select:
//	    <-closed       -> emit every still-open envelope (ReasonShutdown), return
//	    raw := <-in    -> ingest(raw): parse, resolve, route, attach
//	    now := <-ticker -> sweep(now): close expired envelopes, retry
//	                       unresolved slab/magrtrsrv records and resolver lookups
//
// scheduler and magnum lines carry the (src, dst) Key directly. slab and
// magrtrsrv lines carry only a hostname and a "DST n" number; resolving them
// requires an already-open envelope for their destination (via the injected
// Resolver's Lookup(dstUUID) — see resolver.go) — they can never open an
// envelope themselves. A slab record additionally requires its own multicast
// address to match whatever a magrtrsrv record already reported for that
// envelope, once one has — hostname/DST# plus multicast together, for extra
// specificity; before any magrtrsrv has reported one, hostname/DST# alone is
// enough. See resolveMember.
package correlator

import (
	"fmt"
	"sync"
	"time"
)

// Config configures a correlation Engine.
type Config struct {
	// OpenOn is the Kind that opens a new envelope in the normal case. Must
	// be KindScheduler, KindMagnumSubscribe, or KindMagnumComplete — slab
	// and magrtrsrv are member-only kinds and can never open an envelope.
	OpenOn Kind
	// CloseAfter is an envelope's fixed lifetime from OpenedAt; the primary
	// close trigger, alongside supersession, MaxOpen eviction, and shutdown.
	CloseAfter time.Duration
	// SweepInterval is how often the actor scans open envelopes for expiry,
	// retries resolver lookups that had nothing at open time, and retries
	// buffered slab/magrtrsrv records.
	SweepInterval time.Duration
	// MaxOpen caps simultaneously open envelopes; the oldest (by OpenedAt)
	// is evicted with ReasonMaxOpen to make room.
	MaxOpen int
	// PendingWait is how long a slab/magrtrsrv record with no matching open
	// envelope waits before it is dropped. Zero drops it immediately.
	PendingWait time.Duration
	// InputBuffer is the Submit/TrySubmit channel's buffer depth.
	InputBuffer int
}

// DefaultConfig returns the package's default Config.
func DefaultConfig() Config {
	return Config{
		OpenOn:        KindScheduler,
		CloseAfter:    2 * time.Minute,
		SweepInterval: 1 * time.Second,
		MaxOpen:       4096,
		PendingWait:   5 * time.Second,
		InputBuffer:   1024,
	}
}

// applyDefaults fills non-positive fields from DefaultConfig(); a zero
// OpenOn becomes KindScheduler.
func applyDefaults(cfg Config) Config {
	def := DefaultConfig()
	if cfg.OpenOn == KindUnknown {
		cfg.OpenOn = def.OpenOn
	}
	if cfg.CloseAfter <= 0 {
		cfg.CloseAfter = def.CloseAfter
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = def.SweepInterval
	}
	if cfg.MaxOpen <= 0 {
		cfg.MaxOpen = def.MaxOpen
	}
	if cfg.PendingWait < 0 {
		cfg.PendingWait = 0
	}
	if cfg.InputBuffer <= 0 {
		cfg.InputBuffer = def.InputBuffer
	}
	return cfg
}

// Option configures an Engine at construction time.
type Option func(*Engine)

// WithResolver injects the slab-matching Resolver. If omitted, the engine
// uses a resolver that always misses: scheduler/magnum correlation still
// works, but slab/magrtrsrv records never resolve.
func WithResolver(r Resolver) Option {
	return func(e *Engine) { e.resolver = r }
}

// WithMetadataResolver injects an optional MetadataResolver for per-UUID
// enrichment. If omitted, ResolvedAttrs.SrcMeta/DstMeta are never populated.
func WithMetadataResolver(r MetadataResolver) Option {
	return func(e *Engine) { e.meta = r }
}

// WithClock injects the engine's time source. Defaults to RealClock{}.
func WithClock(c Clock) Option {
	return func(e *Engine) { e.clock = c }
}

// WithParsers overrides the parser set tried, in order, by the engine.
// Defaults to DefaultParsers().
func WithParsers(p ...Parser) Option {
	return func(e *Engine) { e.parsers = p }
}

// Stats is a snapshot of the engine's internal counters, safe to read from
// any goroutine at any time.
type Stats struct {
	Submitted        int64 // every RawLog the actor has processed
	Discarded        int64 // no parser matched (expected to dominate)
	ParseErrors      int64 // a parser matched the shape but couldn't extract the body
	UnresolvedSlab   int64 // slab/magrtrsrv record with no matching open envelope yet
	PendingDropped   int64 // a buffered record's pending_wait expired
	EnvelopesOpened  int64
	EnvelopesClosed  int64
	MaxOpenEvictions int64
	Sweeps           int64 // completed sweep() calls; useful to fence tests on a tick finishing
	Ingests          int64 // completed ingest() calls (regardless of outcome); useful to fence
	// tests on a Submit()'d RawLog having been fully parsed, resolved, and routed
}

// pendingRecord is a slab/magrtrsrv Record buffered because no open envelope
// yet matches its {hostname, DstNum}.
type pendingRecord struct {
	rec      Record
	deadline time.Time
}

// Engine is the correlation engine. Construct with New; it owns a goroutine
// until Close is called.
type Engine struct {
	cfg      Config
	parsers  []Parser
	resolver Resolver
	meta     MetadataResolver // nil if not configured
	clock    Clock

	in     chan RawLog
	out    chan *Envelope
	closed chan struct{}
	once   sync.Once

	stats statsCounters

	// actor-owned state — touched only from run() and its helpers.
	open      map[Key]*Envelope
	openByDst map[string]Key    // Dst -> the one currently-open Key for that destination
	slabIndex map[SlabRef]Key   // {hostname,DstNum} -> Key, for every currently-open envelope
	envSlabs  map[Key][]SlabRef // the SlabRefs registered for each open envelope
	pending   []pendingRecord
}

// New constructs and starts a correlation Engine. The returned Engine owns a
// goroutine; call Close when done, and keep draining Events() until it
// closes so the actor's final shutdown sends can complete.
func New(cfg Config, opts ...Option) (*Engine, error) {
	cfg = applyDefaults(cfg)
	switch cfg.OpenOn {
	case KindScheduler, KindMagnumSubscribe, KindMagnumComplete:
	default:
		return nil, fmt.Errorf("correlator: OpenOn must be scheduler, magnum_subscribe, or magnum_complete, got %v", cfg.OpenOn)
	}

	e := &Engine{
		cfg:       cfg,
		parsers:   DefaultParsers(),
		resolver:  nopResolver{},
		clock:     RealClock{},
		in:        make(chan RawLog, cfg.InputBuffer),
		out:       make(chan *Envelope),
		closed:    make(chan struct{}),
		open:      make(map[Key]*Envelope),
		openByDst: make(map[string]Key),
		slabIndex: make(map[SlabRef]Key),
		envSlabs:  make(map[Key][]SlabRef),
	}
	for _, opt := range opts {
		opt(e)
	}

	go e.run()
	return e, nil
}

// Submit hands raw to the engine, blocking until it is accepted or the
// engine is closed. It returns false if the engine has been closed.
func (e *Engine) Submit(raw RawLog) bool {
	// Check closed first, non-blocking: e.in is buffered, so once Close has
	// been called a plain two-case select could still pick "send" at random
	// (both cases ready) instead of reliably reporting the engine as closed.
	select {
	case <-e.closed:
		return false
	default:
	}
	select {
	case <-e.closed:
		return false
	case e.in <- raw:
		return true
	}
}

// TrySubmit is the non-blocking form of Submit.
func (e *Engine) TrySubmit(raw RawLog) bool {
	select {
	case <-e.closed:
		return false
	default:
	}
	select {
	case <-e.closed:
		return false
	case e.in <- raw:
		return true
	default:
		return false
	}
}

// Events returns the channel closed envelopes are dispatched on. It is
// closed once the engine has fully shut down after Close — callers should
// keep reading it until then, including any envelopes still open at
// shutdown time (dispatched with ReasonShutdown).
func (e *Engine) Events() <-chan *Envelope { return e.out }

// Close stops the engine. It is idempotent and safe to call more than once.
// Close itself does not block; the actor goroutine finishes asynchronously
// once its final sends to Events() are received.
func (e *Engine) Close() {
	e.once.Do(func() { close(e.closed) })
}

// Stats returns a snapshot of the engine's internal counters.
func (e *Engine) Stats() Stats { return e.stats.snapshot() }

// run is the engine's single actor goroutine: it owns every field below
// "actor-owned state" in Engine, so none of it needs locking.
func (e *Engine) run() {
	defer close(e.out)

	ticker := e.clock.NewTicker(e.cfg.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.closed:
			for key, env := range e.open {
				e.emit(key, env, ReasonShutdown)
			}
			return
		case raw := <-e.in:
			e.ingest(raw)
		case now := <-ticker.C():
			e.sweep(now)
		}
	}
}

// ingest matches raw against the parser set (first match wins) and routes
// each resulting Record.
func (e *Engine) ingest(raw RawLog) {
	defer e.stats.ingests.Add(1) // last action, after routing/attaching — see Stats.Ingests
	e.stats.submitted.Add(1)

	var (
		matched Parser
		recs    []Record
		err     error
	)
	for _, p := range e.parsers {
		recs, err = p.Parse(raw)
		if err == ErrNoMatch {
			continue
		}
		matched = p
		break
	}
	if matched == nil {
		e.stats.discarded.Add(1)
		return
	}
	if err != nil {
		e.stats.parseErrors.Add(1)
		return
	}

	for i := range recs {
		recs[i].Source = matched.Name()
		if recs[i].Time.IsZero() {
			recs[i].Time = raw.Time
		}
	}

	for _, rec := range recs {
		if rec.Kind == KindSlab || rec.Kind == KindMagrtrsrv {
			key, ok := e.resolveMember(rec)
			if !ok {
				e.stats.unresolvedSlab.Add(1)
				e.bufferPending(rec)
				continue
			}
			rec.Key = &key
		}
		if rec.Key == nil {
			continue
		}
		e.route(rec)
	}
}

// resolveMember resolves a UUID-less slab/magrtrsrv record to its Key via
// hostname+DstNum against slabIndex (sourced from the injected Resolver's
// destination slab list) — the sole mechanism for magrtrsrv, and the
// fallback for slab when its destination has no magrtrsrv-reported multicast
// yet. Once an envelope does have one (env.Resolved.Multicast, only ever set
// by a magrtrsrv record — see attach), a slab record must ALSO carry that
// same multicast to resolve: hostname/DST# plus multicast together, for
// extra specificity. A slab record whose own ADDR conflicts with it is
// treated as unresolved (buffers, eventually drops), not merely deprioritized.
func (e *Engine) resolveMember(rec Record) (Key, bool) {
	ref := SlabRef{Hostname: rec.Slab.Hostname, DstNum: rec.Slab.DstNum}
	key, ok := e.slabIndex[ref]
	if !ok {
		return Key{}, false
	}
	if rec.Kind == KindSlab {
		if env, open := e.open[key]; open && env.Resolved.Multicast != "" && rec.Slab.Multicast != env.Resolved.Multicast {
			return Key{}, false
		}
	}
	return key, true
}

// route attaches rec to its envelope, opening one first if rec's Kind is
// allowed to (scheduler/magnum) and none exists yet.
func (e *Engine) route(rec Record) {
	key := *rec.Key
	env, ok := e.open[key]
	if !ok {
		if rec.Kind == KindSlab || rec.Kind == KindMagrtrsrv {
			// Unreachable in practice: slabIndex only ever yields keys for
			// envelopes that are currently open (registered/torn down
			// together in openEnvelope/emit) — guarded defensively.
			return
		}
		env = e.openEnvelope(key, rec)
		env.Partial = rec.Kind != e.cfg.OpenOn
	} else if env.Partial && rec.Kind == e.cfg.OpenOn {
		env.Partial = false
	}
	e.attach(env, rec)
}

// openEnvelope creates a new envelope for key, superseding any other
// currently-open envelope for the same destination, and registers its
// resolver-derived slab list and metadata.
func (e *Engine) openEnvelope(key Key, rec Record) *Envelope {
	if len(e.open) >= e.cfg.MaxOpen {
		e.evictOldest()
	}

	if prev, ok := e.openByDst[key.Dst]; ok && prev != key {
		if prevEnv, ok := e.open[prev]; ok {
			superseder := key
			prevEnv.SupersededBy = &superseder
			e.emit(prev, prevEnv, ReasonSuperseded)
		}
	}

	env := &Envelope{
		Key:      key,
		OpenedBy: rec.Kind,
		OpenedAt: rec.Time,
		LastAt:   rec.Time,
	}
	e.openByDst[key.Dst] = key
	e.registerSlabs(key, env)

	if e.meta != nil {
		if m, ok := e.meta.Metadata(key.Src, RoleSrc); ok {
			env.Resolved.SrcMeta = m
		}
		if m, ok := e.meta.Metadata(key.Dst, RoleDst); ok {
			env.Resolved.DstMeta = m
		}
	}

	e.open[key] = env
	e.stats.envelopesOpened.Add(1)
	e.drainPendingFor(key)
	return env
}

// registerSlabs looks up key's destination and, if the resolver has
// something for it, records the slab list on env and indexes it in
// slabIndex/envSlabs. Called at open time and again on each sweep for
// envelopes still lacking a slab list.
func (e *Engine) registerSlabs(key Key, env *Envelope) {
	slabs, ok := e.resolver.Lookup(key.Dst)
	if !ok || len(slabs) == 0 {
		return
	}
	env.Resolved.Slabs = slabs
	e.envSlabs[key] = slabs
	for _, ref := range slabs {
		e.slabIndex[ref] = key
	}
}

// evictOldest closes the envelope with the earliest OpenedAt, to make room
// under MaxOpen.
func (e *Engine) evictOldest() {
	var oldestKey Key
	var oldestEnv *Envelope
	for k, env := range e.open {
		if oldestEnv == nil || env.OpenedAt.Before(oldestEnv.OpenedAt) {
			oldestKey, oldestEnv = k, env
		}
	}
	if oldestEnv != nil {
		e.stats.maxOpenEvictions.Add(1)
		e.emit(oldestKey, oldestEnv, ReasonMaxOpen)
	}
}

// attach appends rec to env, extends its time span, and folds in a
// multicast address if rec carries one (magrtrsrv always wins, and updates
// what future slab records must additionally match — see resolveMember;
// slab only fills an empty value, i.e. before any magrtrsrv has reported one).
func (e *Engine) attach(env *Envelope, rec Record) {
	env.Records = append(env.Records, rec)
	if rec.Time.After(env.LastAt) {
		env.LastAt = rec.Time
	}
	if rec.Time.Before(env.OpenedAt) {
		env.OpenedAt = rec.Time
	}
	if rec.Slab != nil && rec.Slab.Multicast != "" {
		if rec.Kind == KindMagrtrsrv || env.Resolved.Multicast == "" {
			env.Resolved.Multicast = rec.Slab.Multicast
		}
	}
}

// sweep closes expired envelopes, retries resolver lookups for envelopes
// still lacking a slab list, and retries (or expires) buffered
// slab/magrtrsrv records.
func (e *Engine) sweep(now time.Time) {
	defer e.finishSweep()

	for key, env := range e.open {
		if now.Sub(env.OpenedAt) >= e.cfg.CloseAfter {
			e.emit(key, env, ReasonCloseAfter)
			continue
		}
		if len(e.envSlabs[key]) == 0 {
			e.registerSlabs(key, env)
		}
	}

	if len(e.pending) == 0 {
		return
	}
	kept := e.pending[:0]
	for _, pr := range e.pending {
		if key, ok := e.resolveMember(pr.rec); ok {
			pr.rec.Key = &key
			e.route(pr.rec)
			continue
		}
		if now.After(pr.deadline) {
			e.stats.pendingDropped.Add(1)
			continue
		}
		kept = append(kept, pr)
	}
	e.pending = kept
}

// finishSweep marks one sweep() call complete. Incrementing it last (after
// all of sweep's work, including the pending retry) lets tests fence on a
// specific sweep tick having fully finished, via Stats().Sweeps.
func (e *Engine) finishSweep() { e.stats.sweeps.Add(1) }

// bufferPending holds rec, a slab/magrtrsrv record with no matching open
// envelope yet, for up to PendingWait before it is dropped.
func (e *Engine) bufferPending(rec Record) {
	if e.cfg.PendingWait <= 0 {
		e.stats.pendingDropped.Add(1)
		return
	}
	e.pending = append(e.pending, pendingRecord{
		rec:      rec,
		deadline: e.clock.Now().Add(e.cfg.PendingWait),
	})
}

// drainPendingFor re-checks buffered records against key's just-registered
// slabIndex entries immediately, so they don't wait for the next sweep tick.
// (A magrtrsrv record setting Resolved.Multicast in attach never needs this:
// the multicast cross-check in resolveMember only tightens a slab record
// that has already resolved via slabIndex — it can't unlock one that hasn't.)
func (e *Engine) drainPendingFor(key Key) {
	if len(e.pending) == 0 {
		return
	}
	kept := e.pending[:0]
	for _, pr := range e.pending {
		if k, ok := e.resolveMember(pr.rec); ok && k == key {
			pr.rec.Key = &k
			e.route(pr.rec)
			continue
		}
		kept = append(kept, pr)
	}
	e.pending = kept
}

// emit closes and dispatches env, tearing down its slabIndex/openByDst
// registrations first. It blocks sending on Events() until received —
// callers must keep draining Events(), including through and after Close(),
// per its doc comment.
func (e *Engine) emit(key Key, env *Envelope, reason CloseReason) {
	delete(e.open, key)
	if e.openByDst[key.Dst] == key {
		delete(e.openByDst, key.Dst)
	}
	for _, ref := range e.envSlabs[key] {
		if e.slabIndex[ref] == key { // compare-and-delete: a newer envelope for the
			delete(e.slabIndex, ref) // same destination may already own this ref
		}
	}
	delete(e.envSlabs, key)

	env.Reason = reason
	e.stats.envelopesClosed.Add(1)
	e.out <- env
}
