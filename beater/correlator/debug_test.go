package correlator

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// eventLog collects UnresolvedEvents. The observer runs on the engine's actor
// goroutine, the test reads from another, hence the mutex.
type eventLog struct {
	mu     sync.Mutex
	events []UnresolvedEvent
}

func (l *eventLog) observe(ev UnresolvedEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *eventLog) snapshot() []UnresolvedEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]UnresolvedEvent(nil), l.events...)
}

func TestMissReasonString(t *testing.T) {
	require.Equal(t, "resolved", MissNone.String())
	require.Equal(t, "no_slab_match", MissNoSlabMatch.String())
	require.Equal(t, "multicast_conflict", MissMulticastConflict.String())
}

func TestEngine_UnresolvedNoSlabMatch_NoEnvelopeOpen(t *testing.T) {
	var log eventLog
	eng, clock := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute },
		WithResolver(NewMapResolver()), WithUnresolvedObserver(log.observe))

	submitAll(t, eng, RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime, Hostname: "sv7bc-slab058"})

	require.EqualValues(t, 1, eng.Stats().UnresolvedSlab)
	require.EqualValues(t, 1, eng.Stats().UnresolvedNoSlabMatch)
	require.EqualValues(t, 0, eng.Stats().UnresolvedMulticastConflict)

	evs := log.snapshot()
	require.Len(t, evs, 1)
	require.False(t, evs[0].Dropped)
	require.Equal(t, MissNoSlabMatch, evs[0].Reason)
	require.Equal(t, KindSlab, evs[0].Kind)
	require.Equal(t, "sv7bc-slab058", evs[0].Hostname)
	require.Equal(t, 4, evs[0].DstNum)
	require.Equal(t, "239.32.111.55", evs[0].Multicast)
	require.Equal(t, 0, evs[0].OpenEnvelopes) // nothing in flight: the slab log has no route to attach to

	// Past pending_wait it is dropped, and the drop event carries the final reason.
	clock.Advance(2 * time.Minute)
	waitStat(t, eng, func(s Stats) int64 { return s.PendingDropped }, 1)
	require.EqualValues(t, 1, eng.Stats().DroppedNoSlabMatch)

	evs = log.snapshot()
	require.Len(t, evs, 2)
	require.True(t, evs[1].Dropped)
	require.Equal(t, MissNoSlabMatch, evs[1].Reason)
	require.Equal(t, 2*time.Minute, evs[1].Waited)
}

func TestEngine_UnresolvedNoSlabMatch_ResolverHasNothing(t *testing.T) {
	var log eventLog
	// Envelope opens, but the resolver returns nothing for its destination.
	eng, _ := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute },
		WithResolver(NewMapResolver()), WithUnresolvedObserver(log.observe))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(time.Second), Hostname: "sv7bc-slab058"},
	)

	evs := log.snapshot()
	require.Len(t, evs, 1)
	require.Equal(t, MissNoSlabMatch, evs[0].Reason)
	require.Equal(t, 1, evs[0].OpenEnvelopes)    // a route IS in flight...
	require.Equal(t, 1, evs[0].OpenWithoutSlabs) // ...but the resolver gave it no slab list
	require.Empty(t, evs[0].KnownDstNumsForHostname)
	require.Empty(t, evs[0].KnownHostnamesForDstNum)
}

func TestEngine_UnresolvedNoSlabMatch_HostnameKnownUnderOtherDstNum(t *testing.T) {
	var log eventLog
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{
		{Hostname: "sv7bc-slab058", DstNum: 3},
		{Hostname: "sv7bc-slab027", DstNum: 4},
	}}})
	eng, _ := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute },
		WithResolver(res), WithUnresolvedObserver(log.observe))

	// The log says slab058 / DST 4, but the resolver has slab058 as DST 3 and slab027 as DST 4.
	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(time.Second), Hostname: "sv7bc-slab058"},
	)

	evs := log.snapshot()
	require.Len(t, evs, 1)
	require.Equal(t, MissNoSlabMatch, evs[0].Reason)
	require.Equal(t, 1, evs[0].OpenEnvelopes)
	require.Equal(t, 0, evs[0].OpenWithoutSlabs)
	require.Equal(t, []int{3}, evs[0].KnownDstNumsForHostname)
	require.Equal(t, []string{"sv7bc-slab027"}, evs[0].KnownHostnamesForDstNum)
}

func TestEngine_UnresolvedMulticastConflict(t *testing.T) {
	var log eventLog
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab058", DstNum: 4}}}})
	// close_after stays well past the clock advance below: otherwise the envelope
	// closes on the same sweep, before the pending-drop step, and the actor
	// blocks handing it to an Events() nobody is reading here.
	eng, clock := newTestEngine(t, func(c *Config) {
		c.PendingWait = time.Minute
		c.CloseAfter = time.Hour
	}, WithResolver(res), WithUnresolvedObserver(log.observe))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: magrtrsrvLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(time.Second)},
		RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.99"), Time: testTime.Add(2 * time.Second), Hostname: "sv7bc-slab058"},
	)

	require.EqualValues(t, 1, eng.Stats().UnresolvedMulticastConflict)
	require.EqualValues(t, 0, eng.Stats().UnresolvedNoSlabMatch)

	evs := log.snapshot()
	require.Len(t, evs, 1)
	require.Equal(t, MissMulticastConflict, evs[0].Reason)
	require.Equal(t, "239.32.111.99", evs[0].Multicast)         // what the slab log said
	require.Equal(t, "239.32.111.55", evs[0].ExpectedMulticast) // what magrtrsrv reported
	require.NotNil(t, evs[0].MatchedKey)
	require.Equal(t, Key{Src: testSrc, Dst: testDst}, *evs[0].MatchedKey)

	clock.Advance(2 * time.Minute)
	waitStat(t, eng, func(s Stats) int64 { return s.PendingDropped }, 1)
	require.EqualValues(t, 1, eng.Stats().DroppedMulticastConflict)
	require.EqualValues(t, 0, eng.Stats().DroppedNoSlabMatch)
}

func TestEngine_UnresolvedPendingWaitZeroDropsImmediately(t *testing.T) {
	var log eventLog
	eng, _ := newTestEngine(t, func(c *Config) { c.PendingWait = 0 },
		WithResolver(NewMapResolver()), WithUnresolvedObserver(log.observe))

	submitAll(t, eng, RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime, Hostname: "sv7bc-slab058"})

	require.EqualValues(t, 1, eng.Stats().UnresolvedSlab)
	require.EqualValues(t, 1, eng.Stats().PendingDropped)
	require.EqualValues(t, 1, eng.Stats().DroppedNoSlabMatch)

	evs := log.snapshot()
	require.Len(t, evs, 1) // a single event, already marked dropped
	require.True(t, evs[0].Dropped)
}

func TestEngine_NoObserverStillCountsReasons(t *testing.T) {
	// Counters must work with no observer installed (the default).
	eng, _ := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute }, WithResolver(NewMapResolver()))

	submitAll(t, eng, RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime, Hostname: "sv7bc-slab058"})

	require.EqualValues(t, 1, eng.Stats().UnresolvedNoSlabMatch)
}
