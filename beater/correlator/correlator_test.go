package correlator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- test helpers ------------------------------------------------------

func newTestEngine(t *testing.T, mutate func(*Config), opts ...Option) (*Engine, *fakeClock) {
	t.Helper()
	clock := newFakeClock(testTime)
	cfg := Config{
		OpenOn:        KindScheduler,
		CloseAfter:    time.Minute,
		SweepInterval: time.Second, // irrelevant with fakeClock; ticks only fire on Advance
		MaxOpen:       10,
		PendingWait:   200 * time.Millisecond,
		InputBuffer:   16,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	allOpts := append([]Option{WithClock(clock)}, opts...)
	eng, err := New(cfg, allOpts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		eng.Close()
		for range eng.Events() { // drain until closed so the actor's shutdown sends never block forever
		}
	})
	return eng, clock
}

// submitAll submits every raw in order and then blocks until the engine has
// fully ingested (parsed, resolved, routed, attached) all of them — using
// Stats().Ingests, which increments once per fully-completed ingest() call,
// as an exact counter: e.in is FIFO with a single sender and the actor
// processes one ingest() at a time, so waiting for the count to increase by
// exactly len(raws) unambiguously covers this whole batch (a "some later
// discard incremented some counter" style check would not: with several
// discardable lines in one batch, an earlier one could satisfy a "+1"
// threshold well before the batch's later lines — or the fence's own probe —
// have actually been processed).
func submitAll(t *testing.T, eng *Engine, raws ...RawLog) {
	t.Helper()
	before := eng.Stats().Ingests
	for _, raw := range raws {
		require.True(t, eng.Submit(raw))
	}
	waitStat(t, eng, func(s Stats) int64 { return s.Ingests }, before+int64(len(raws)))
}

func recvEnvelope(t *testing.T, eng *Engine) *Envelope {
	t.Helper()
	select {
	case env := <-eng.Events():
		require.NotNil(t, env, "Events() closed unexpectedly")
		return env
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for envelope")
		return nil
	}
}

func expectNoEnvelope(t *testing.T, eng *Engine, wait time.Duration) {
	t.Helper()
	select {
	case env := <-eng.Events():
		t.Fatalf("unexpected envelope: key=%+v reason=%s", env.Key, env.Reason)
	case <-time.After(wait):
	}
}

func waitStat(t *testing.T, eng *Engine, get func(Stats) int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := get(eng.Stats()); got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for stat to reach %d, got %d", want, get(eng.Stats()))
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func schedulerLine(src, dst string) string {
	return `main: Sending route: [[{'src': ['` + src + `'], 'dst': ['` + dst + `']}]]`
}

func magnumALine(src, dst string) string {
	return `INFO:jsonrpc:Subscribe request. Dst [('` + dst + `',)], Sub [('` + src + `',)], User [None], ID [None]`
}

func magnumBLine(src, dst string) string {
	return `INFO:subscription:Subscription Request Complete: Routes [1-1]: [{'dst': ['` + dst + `'], 'sub_dst': ['` + src + `']}]`
}

func slabLine(hostname string, dstNum int, multicast string) string {
	return fmt.Sprintf(`%s [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: AuditSet:DST %d ADDR:"%s;sync-time=3000"`, hostname, dstNum, multicast)
}

func magrtrsrvLine(hostname string, output int, multicast string) string {
	return fmt.Sprintf(`INFO:commands:Cmd. D [1051], N [%s], M [set.rx.route], A [[[{'dest': {'output': %d}, 'sources': [{'multicast_ip': '%s'}]}]]], K [{}]`, hostname, output, multicast)
}

// --- tests ---------------------------------------------------------------

func TestEngine_OpenerCreatesEnvelope(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, Key{Src: testSrc, Dst: testDst}, env.Key)
	require.Equal(t, KindScheduler, env.OpenedBy)
	require.False(t, env.Partial)
	require.Equal(t, ReasonCloseAfter, env.Reason)
	require.Len(t, env.Records, 1)
	require.Equal(t, time.Duration(0), env.Duration())
}

func TestEngine_NonOpenerAttachesAndSpansDuration(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	t0 := testTime
	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: t0},
		RawLog{Line: magnumALine(testSrc, testDst), Time: t0.Add(5 * time.Second)},
		RawLog{Line: magnumBLine(testSrc, testDst), Time: t0.Add(6 * time.Second)},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Len(t, env.Records, 3)
	require.Equal(t, 6*time.Second, env.Duration())
	require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2}, env.SourceCounts())
}

func TestEngine_SchedulerToSlabMillis(t *testing.T) {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}}})
	eng, clock := newTestEngine(t, nil, WithResolver(res))

	t0 := testTime
	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: t0},
		RawLog{Line: magnumALine(testSrc, testDst), Time: t0.Add(20 * time.Second)}, // later than the slab log
		RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: t0.Add(12 * time.Second), Hostname: "sv7bc-slab027"},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, 20*time.Second, env.Duration()) // Duration includes the later magnum record

	ms, ok := env.SchedulerToSlabMillis()
	require.True(t, ok)
	require.EqualValues(t, 12_000, ms) // ignores the later magnum record entirely
}

func TestEngine_SchedulerToSlabMillis_MissingEitherSide(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	t.Run("no slab yet", func(t *testing.T) {
		submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
		clock.Advance(time.Minute)
		env := recvEnvelope(t, eng)
		_, ok := env.SchedulerToSlabMillis()
		require.False(t, ok)
	})

	t.Run("magnum-opened partial with no scheduler", func(t *testing.T) {
		src2 := "88888888-8888-8888-8888-888888888888"
		submitAll(t, eng, RawLog{Line: magnumALine(src2, testDst), Time: testTime})
		clock.Advance(time.Minute)
		env := recvEnvelope(t, eng)
		require.True(t, env.Partial)
		_, ok := env.SchedulerToSlabMillis()
		require.False(t, ok)
	})
}

func TestEngine_TwoSlabLogsOneEnvelope(t *testing.T) {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{
		{Hostname: "sv7bc-slab027", DstNum: 4},
		{Hostname: "sv7bc-slab058", DstNum: 4},
	}}})
	eng, clock := newTestEngine(t, nil, WithResolver(res))

	t0 := testTime
	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: t0},
		RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: t0.Add(12 * time.Second), Hostname: "sv7bc-slab027"},
		RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: t0.Add(13 * time.Second), Hostname: "sv7bc-slab058"},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, 2, env.SourceCounts()["slab"])
	require.Equal(t, t0.Add(13*time.Second), env.LastAt)
}

func TestEngine_OneSlabLogStillCloses(t *testing.T) {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}}})
	eng, clock := newTestEngine(t, nil, WithResolver(res))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: testTime.Add(5 * time.Second), Hostname: "sv7bc-slab027"},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, 1, env.SourceCounts()["slab"])
	require.Equal(t, ReasonCloseAfter, env.Reason)
}

// TestEngine_MagrtrsrvDictatesSlabMulticast covers the compound match rule:
// hostname/DST# alone (via the resolver's slab list) is enough for a slab
// record to attach ONLY until a magrtrsrv record has reported a multicast
// for that destination; once one has, a slab record must carry that exact
// multicast too, or it is treated as unresolved rather than attaching with
// a conflicting value.
func TestEngine_MagrtrsrvDictatesSlabMulticast(t *testing.T) {
	newResolverEngine := func(t *testing.T) (*Engine, *fakeClock) {
		res := NewMapResolver()
		res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab058", DstNum: 4}}}})
		return newTestEngine(t, nil, WithResolver(res))
	}

	t.Run("slab before magrtrsrv falls back to hostname/DST# alone, then magrtrsrv overwrites the multicast", func(t *testing.T) {
		eng, clock := newResolverEngine(t)

		submitAll(t, eng,
			RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
			RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.99"), Time: testTime.Add(time.Second), Hostname: "sv7bc-slab058"},
			RawLog{Line: magrtrsrvLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(2 * time.Second)},
		)
		clock.Advance(time.Minute)

		env := recvEnvelope(t, eng)
		require.Equal(t, 1, env.SourceCounts()["slab"])           // attached via hostname/DST# fallback, no magrtrsrv yet
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // magrtrsrv still overwrites afterward
	})

	t.Run("once magrtrsrv is known, a slab log with the matching multicast attaches", func(t *testing.T) {
		eng, clock := newResolverEngine(t)

		submitAll(t, eng,
			RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
			RawLog{Line: magrtrsrvLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(time.Second)},
			RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(2 * time.Second), Hostname: "sv7bc-slab058"},
		)
		clock.Advance(time.Minute)

		env := recvEnvelope(t, eng)
		require.Equal(t, 1, env.SourceCounts()["slab"])
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast)
	})

	t.Run("once magrtrsrv is known, a slab log with a conflicting multicast is rejected, not overridden", func(t *testing.T) {
		eng, clock := newResolverEngine(t)

		submitAll(t, eng,
			RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
			RawLog{Line: magrtrsrvLine("sv7bc-slab058", 4, "239.32.111.55"), Time: testTime.Add(time.Second)},
			RawLog{Line: slabLine("sv7bc-slab058", 4, "239.32.111.99"), Time: testTime.Add(2 * time.Second), Hostname: "sv7bc-slab058"},
		)
		require.EqualValues(t, 1, eng.Stats().UnresolvedSlab) // hostname/DST# matched, but the multicast didn't

		clock.Advance(time.Minute) // also past pending_wait, so the mismatched slab record gets dropped, not retried forever
		env := recvEnvelope(t, eng)

		require.Equal(t, 0, env.SourceCounts()["slab"])           // never attached
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // untouched by the rejected slab record
		require.Equal(t, 1, env.SourceCounts()["magrtrsrv"])
		require.EqualValues(t, 1, eng.Stats().PendingDropped)
	})
}

func TestEngine_SlabNeverOpensEnvelope(t *testing.T) {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}}})
	eng, clock := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute }, WithResolver(res))

	// A slab log arrives first — no envelope open yet for testDst.
	submitAll(t, eng, RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: testTime, Hostname: "sv7bc-slab027"})
	require.EqualValues(t, 1, eng.Stats().UnresolvedSlab)
	require.EqualValues(t, 0, eng.Stats().EnvelopesOpened)

	// Past pending_wait it is dropped, not opened.
	clock.Advance(2 * time.Minute)
	waitStat(t, eng, func(s Stats) int64 { return s.PendingDropped }, 1)
	require.EqualValues(t, 0, eng.Stats().EnvelopesOpened)

	expectNoEnvelope(t, eng, 50*time.Millisecond)
}

func TestEngine_SlabAttachesOnceOpenerArrives(t *testing.T) {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}}})
	eng, clock := newTestEngine(t, func(c *Config) { c.PendingWait = time.Minute }, WithResolver(res))

	submitAll(t, eng, RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: testTime, Hostname: "sv7bc-slab027"})
	require.EqualValues(t, 1, eng.Stats().UnresolvedSlab)

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime.Add(time.Second)})

	clock.Advance(2 * time.Minute)
	env := recvEnvelope(t, eng)
	require.Len(t, env.Records, 2)
	require.Equal(t, 1, env.SourceCounts()["slab"])
}

func TestEngine_ResolverCatchesUpOnSweep(t *testing.T) {
	res := NewMapResolver() // empty at first
	eng, clock := newTestEngine(t, func(c *Config) {
		c.PendingWait = time.Minute
		c.CloseAfter = time.Hour
	}, WithResolver(res))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: slabLine("sv7bc-slab027", 4, "239.32.111.55"), Time: testTime.Add(time.Second), Hostname: "sv7bc-slab027"},
	)
	require.EqualValues(t, 1, eng.Stats().UnresolvedSlab)
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened)

	// The resolver catches up; the next sweep tick should retry and attach.
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}}})
	clock.Advance(time.Second)
	waitStat(t, eng, func(s Stats) int64 { return s.Sweeps }, 1)

	// Now close it out.
	clock.Advance(time.Hour)
	env := recvEnvelope(t, eng)
	require.Equal(t, 1, env.SourceCounts()["slab"])
	require.Equal(t, []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}}, env.Resolved.Slabs)
}

func TestEngine_Supersession(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Hour })

	src2 := "66666666-6666-6666-6666-666666666666"

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened)

	require.True(t, eng.Submit(RawLog{Line: schedulerLine(src2, testDst), Time: testTime.Add(time.Second)}))

	// The first envelope is dispatched immediately as superseded — no clock advance needed.
	env := recvEnvelope(t, eng)
	require.Equal(t, Key{Src: testSrc, Dst: testDst}, env.Key)
	require.Equal(t, ReasonSuperseded, env.Reason)
	require.NotNil(t, env.SupersededBy)
	require.Equal(t, Key{Src: src2, Dst: testDst}, *env.SupersededBy)

	// The second stays open; close it out and confirm no stray state remains.
	clock.Advance(2 * time.Hour)
	env2 := recvEnvelope(t, eng)
	require.Equal(t, Key{Src: src2, Dst: testDst}, env2.Key)
	require.Equal(t, ReasonCloseAfter, env2.Reason)

	stats := eng.Stats()
	require.EqualValues(t, 2, stats.EnvelopesOpened)
	require.EqualValues(t, 2, stats.EnvelopesClosed)
}

func TestEngine_ReissuingSameKeyDoesNotSupersede(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Hour })

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened)

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime.Add(time.Second)})
	expectNoEnvelope(t, eng, 100*time.Millisecond)
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened) // still just one

	clock.Advance(2 * time.Hour)
	env := recvEnvelope(t, eng)
	require.Len(t, env.Records, 2)
	require.Equal(t, ReasonCloseAfter, env.Reason)
}

func TestEngine_SupersessionIsPerDestination(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Hour })

	dst2 := "77777777-7777-7777-7777-777777777777"

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: schedulerLine(testSrc, dst2), Time: testTime.Add(time.Second)},
	)
	expectNoEnvelope(t, eng, 100*time.Millisecond) // neither superseded; different destinations

	clock.Advance(2 * time.Hour)
	env1 := recvEnvelope(t, eng)
	env2 := recvEnvelope(t, eng)
	seen := map[string]bool{env1.Key.Dst: true, env2.Key.Dst: true}
	require.True(t, seen[testDst])
	require.True(t, seen[dst2])
}

func TestEngine_PartialEnvelope(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Hour })

	// magnum arrives first; no scheduler yet.
	submitAll(t, eng, RawLog{Line: magnumALine(testSrc, testDst), Time: testTime})
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened)

	// A late scheduler for the same key attaches and clears Partial.
	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime.Add(time.Second)})

	clock.Advance(2 * time.Hour)
	env := recvEnvelope(t, eng)
	require.False(t, env.Partial)
	require.Equal(t, KindMagnumSubscribe, env.OpenedBy)
	require.Len(t, env.Records, 2)
}

func TestEngine_PartialEnvelopeStaysPartialIfSchedulerNeverArrives(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Hour })

	submitAll(t, eng, RawLog{Line: magnumALine(testSrc, testDst), Time: testTime})
	clock.Advance(2 * time.Hour)

	env := recvEnvelope(t, eng)
	require.True(t, env.Partial)
}

func TestEngine_CloseAfterFiresWhileStillActive(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.CloseAfter = time.Minute })

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})

	clock.Advance(10 * time.Second)
	waitStat(t, eng, func(s Stats) int64 { return s.Sweeps }, 1)
	submitAll(t, eng, RawLog{Line: magnumALine(testSrc, testDst), Time: testTime.Add(10 * time.Second)})

	clock.Advance(49 * time.Second) // total 59s since OpenedAt — not yet
	waitStat(t, eng, func(s Stats) int64 { return s.Sweeps }, 2)
	expectNoEnvelope(t, eng, 50*time.Millisecond)

	clock.Advance(2 * time.Second) // total 61s — now it closes
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonCloseAfter, env.Reason)
	require.Len(t, env.Records, 2)
}

func TestEngine_MaxOpenEviction(t *testing.T) {
	eng, _ := newTestEngine(t, func(c *Config) {
		c.MaxOpen = 2
		c.CloseAfter = time.Hour
	})

	dst2 := "77777777-7777-7777-7777-777777777777"
	dst3 := "99999999-9999-9999-9999-999999999999"

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, dst2), Time: testTime.Add(time.Second)})
	require.EqualValues(t, 2, eng.Stats().EnvelopesOpened)
	require.EqualValues(t, 0, eng.Stats().EnvelopesClosed)

	// A third destination should evict the oldest (testDst) immediately. The
	// eviction's emit() is synchronous within this Submit's own processing and
	// blocks until received, so recvEnvelope (not submitAll/fence) is what
	// unblocks the actor here.
	require.True(t, eng.Submit(RawLog{Line: schedulerLine(testSrc, dst3), Time: testTime.Add(2 * time.Second)}))

	env := recvEnvelope(t, eng)
	require.Equal(t, testDst, env.Key.Dst)
	require.Equal(t, ReasonMaxOpen, env.Reason)
	require.EqualValues(t, 1, eng.Stats().MaxOpenEvictions)
}

func TestEngine_DiscardsUnrelatedLines(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	raws := make([]RawLog, 0, 21)
	for i := 0; i < 20; i++ {
		raws = append(raws, RawLog{Line: fmt.Sprintf("totally unrelated log line %d", i), Time: testTime})
	}
	raws = append(raws, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	submitAll(t, eng, raws...)

	require.EqualValues(t, 20, eng.Stats().Discarded)
	require.EqualValues(t, 1, eng.Stats().EnvelopesOpened)

	clock.Advance(time.Minute)
	env := recvEnvelope(t, eng)
	require.Len(t, env.Records, 1)
}

func TestEngine_ParseErrorCountedNoEnvelope(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	// Matches the scheduler prefilter/gate but has two src UUIDs -> parse error.
	other := "44444444-4444-4444-4444-444444444444"
	bad := `main: Sending route: [[{'src': ['` + testSrc + `', '` + other + `'], 'dst': ['` + testDst + `']}]]`
	submitAll(t, eng, RawLog{Line: bad, Time: testTime})

	require.EqualValues(t, 1, eng.Stats().ParseErrors)
	require.EqualValues(t, 0, eng.Stats().EnvelopesOpened)

	clock.Advance(time.Minute)
	expectNoEnvelope(t, eng, 50*time.Millisecond)
}

func TestEngine_CloseIdempotentAndEventsDrains(t *testing.T) {
	eng, _ := newTestEngine(t, nil)

	require.True(t, eng.Submit(RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime}))
	eng.Close()
	eng.Close() // idempotent, must not panic

	require.False(t, eng.Submit(RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime}))

	// Events() must eventually close (the shutdown envelope, then the channel itself).
	timeout := time.After(2 * time.Second)
	for {
		select {
		case env, ok := <-eng.Events():
			if !ok {
				return // channel closed cleanly
			}
			require.Equal(t, ReasonShutdown, env.Reason)
		case <-timeout:
			t.Fatal("Events() never closed after Close()")
		}
	}
}

func TestEngine_ConcurrentSubmit(t *testing.T) {
	eng, clock := newTestEngine(t, func(c *Config) { c.InputBuffer = 4 })

	done := make(chan struct{})
	go func() {
		for range eng.Events() {
		}
		close(done)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			eng.Submit(RawLog{Line: fmt.Sprintf("unrelated %d", i), Time: testTime})
		}(i)
	}
	wg.Wait()

	clock.Advance(time.Minute)
	eng.Close()
	<-done
}

func TestEngine_MetadataResolver(t *testing.T) {
	meta := MetadataResolverFunc(func(uuid string, role Role) (map[string]any, bool) {
		if role == RoleSrc && uuid == testSrc {
			return map[string]any{"name": "source-one"}, true
		}
		if role == RoleDst && uuid == testDst {
			return map[string]any{"name": "dest-one"}, true
		}
		return nil, false
	})
	eng, clock := newTestEngine(t, nil, WithMetadataResolver(meta))

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, "source-one", env.Resolved.SrcMeta["name"])
	require.Equal(t, "dest-one", env.Resolved.DstMeta["name"])
}

func TestEngine_NoMetadataResolverConfigured(t *testing.T) {
	eng, clock := newTestEngine(t, nil) // no WithMetadataResolver

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Nil(t, env.Resolved.SrcMeta)
	require.Nil(t, env.Resolved.DstMeta)
}

func TestNew_RejectsMemberOnlyOpenOn(t *testing.T) {
	_, err := New(Config{OpenOn: KindSlab})
	require.Error(t, err)

	_, err = New(Config{OpenOn: KindMagrtrsrv})
	require.Error(t, err)
}
