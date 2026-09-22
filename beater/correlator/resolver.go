package correlator

import (
	"sync"
	"sync/atomic"
)

// SlabRef identifies one physical slab endpoint for a destination: a
// hostname and the "DST n" number it answers to.
type SlabRef struct {
	Hostname string
	DstNum   int
}

// Entry is one destination's slab list, as fed to MapResolver.Store/Upsert.
type Entry struct {
	DstUUID string
	Slabs   []SlabRef // the destination's slab list (len 1..N, usually 2)
}

// Resolver is a plain UUID-keyed cache: given a destination UUID, return the
// slab endpoints known for it. That is its entire job — it never runs in
// reverse (no hostname/DST# -> UUID path) and it never holds a multicast
// address. Every multicast the engine ever sees comes from a slab or
// magrtrsrv log line directly, never from here.
//
// Lookup runs synchronously on the engine's single actor goroutine, so an
// implementation must be a fast, local, already-cached read — never a
// network call.
type Resolver interface {
	Lookup(dstUUID string) ([]SlabRef, bool)
}

// ResolverFunc adapts a plain function to Resolver, the same pattern as
// http.HandlerFunc. Wrap a closure over whatever cache the main program
// already owns; the closure does the translation into []SlabRef, so this
// package never needs to know that cache's real shape.
type ResolverFunc func(dstUUID string) ([]SlabRef, bool)

// Lookup calls f.
func (f ResolverFunc) Lookup(dstUUID string) ([]SlabRef, bool) { return f(dstUUID) }

// nopResolver is the Engine's default Resolver when none is injected via
// WithResolver: it always misses, so scheduler/magnum correlation still
// works but slab/magrtrsrv records never resolve.
type nopResolver struct{}

func (nopResolver) Lookup(string) ([]SlabRef, bool) { return nil, false }

// MapResolver is a self-contained in-memory Resolver for callers with no
// existing cache to close over (or for tests). Reads are lock-free (an
// atomic.Pointer swap on write), so Store/Upsert can be called from any
// goroutine while the engine's actor concurrently calls Lookup.
type MapResolver struct {
	mu    sync.Mutex // serializes Store/Upsert; Lookup never touches this
	state atomic.Pointer[map[string][]SlabRef]
}

// NewMapResolver returns an empty MapResolver.
func NewMapResolver() *MapResolver {
	m := &MapResolver{}
	empty := map[string][]SlabRef{}
	m.state.Store(&empty)
	return m
}

// Store atomically replaces the entire table. Any destination present before
// but absent from entries is forgotten.
func (m *MapResolver) Store(entries []Entry) {
	next := make(map[string][]SlabRef, len(entries))
	for _, e := range entries {
		next[e.DstUUID] = e.Slabs
	}
	m.mu.Lock()
	m.state.Store(&next)
	m.mu.Unlock()
}

// Upsert adds or replaces one destination's slab list, leaving the rest of
// the table untouched.
func (m *MapResolver) Upsert(dstUUID string, slabs []SlabRef) {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := *m.state.Load()
	next := make(map[string][]SlabRef, len(old)+1)
	for k, v := range old {
		next[k] = v
	}
	next[dstUUID] = slabs
	m.state.Store(&next)
}

// Lookup returns the slab endpoints known for a destination UUID.
func (m *MapResolver) Lookup(dstUUID string) ([]SlabRef, bool) {
	table := *m.state.Load()
	slabs, ok := table[dstUUID]
	return slabs, ok
}

// Role tells a MetadataResolver whether the UUID it is being asked about is
// a Key.Src or a Key.Dst, in case the main program keeps separate caches for
// each.
type Role uint8

const (
	RoleSrc Role = iota
	RoleDst
)

// String returns "src" or "dst".
func (r Role) String() string {
	if r == RoleDst {
		return "dst"
	}
	return "src"
}

// MetadataResolver returns arbitrary enrichment for a UUID. It is entirely
// independent of Resolver — used for names/tags, not slab matching — and is
// optional: an engine with none configured simply never populates
// ResolvedAttrs.SrcMeta/DstMeta.
//
// Metadata runs synchronously on the engine's single actor goroutine, so an
// implementation must be a fast, local, already-cached read — never a
// network call.
type MetadataResolver interface {
	Metadata(uuid string, role Role) (map[string]any, bool)
}

// MetadataResolverFunc adapts a plain function to MetadataResolver, the same
// closure-adapter pattern as ResolverFunc.
type MetadataResolverFunc func(uuid string, role Role) (map[string]any, bool)

// Metadata calls f.
func (f MetadataResolverFunc) Metadata(uuid string, role Role) (map[string]any, bool) {
	return f(uuid, role)
}
