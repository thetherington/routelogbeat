package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Tests for Config.CloseOnComplete: closing an envelope early once every slab
// in its resolver slab list has logged, after CompleteGrace.

const (
	slabA = "sv7bc-slab027"
	slabB = "sv7bc-slab058"
	mcast = "239.32.111.55"
)

func twoSlabResolver() *MapResolver {
	res := NewMapResolver()
	res.Store([]Entry{{DstUUID: testDst, Slabs: []SlabRef{
		{Hostname: slabA, DstNum: 4},
		{Hostname: slabB, DstNum: 4},
	}}})
	return res
}

func closeOnComplete(grace time.Duration) func(*Config) {
	return func(c *Config) {
		c.CloseOnComplete = true
		c.CompleteGrace = grace
	}
}

func slabRaw(host string, at time.Time) RawLog {
	return RawLog{Line: slabLine(host, 4, mcast), Time: at, Hostname: host}
}

// advanceAndSweep moves the clock and waits for that tick's sweep to finish,
// so a following expectNoEnvelope really means "this sweep closed nothing".
func advanceAndSweep(t *testing.T, eng *Engine, clock *fakeClock, d time.Duration) {
	t.Helper()
	before := eng.Stats().Sweeps
	clock.Advance(d)
	waitStat(t, eng, func(s Stats) int64 { return s.Sweeps }, before+1)
}

func TestCloseOnComplete_OffByDefault(t *testing.T) {
	eng, clock := newTestEngine(t, nil, WithResolver(twoSlabResolver()))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		slabRaw(slabA, testTime.Add(12*time.Second)),
		slabRaw(slabB, testTime.Add(13*time.Second)),
	)
	advanceAndSweep(t, eng, clock, 30*time.Second)
	expectNoEnvelope(t, eng, 50*time.Millisecond)

	clock.Advance(30 * time.Second) // reaches CloseAfter (1m)
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonCloseAfter, env.Reason)
	require.Zero(t, eng.Stats().ClosedComplete)
}

func TestCloseOnComplete_ClosesAfterGraceOnceEverySlabLogged(t *testing.T) {
	eng, clock := newTestEngine(t, closeOnComplete(2*time.Second), WithResolver(twoSlabResolver()))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		slabRaw(slabA, testTime.Add(12*time.Second)),
		slabRaw(slabB, testTime.Add(12588*time.Millisecond)),
	)

	advanceAndSweep(t, eng, clock, time.Second) // inside the grace period
	expectNoEnvelope(t, eng, 50*time.Millisecond)

	clock.Advance(time.Second) // grace elapsed
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonComplete, env.Reason)
	require.Equal(t, 2, env.SourceCounts()["slab"])

	// Measurements come from the records, not from when the envelope closed.
	ms, ok := env.SchedulerToSlabMillis()
	require.True(t, ok)
	require.EqualValues(t, 12588, ms)

	s := eng.Stats()
	require.EqualValues(t, 1, s.ClosedComplete)
	require.EqualValues(t, 1, s.EnvelopesClosed)
}

func TestCloseOnComplete_MissingSlabFallsBackToCloseAfter(t *testing.T) {
	eng, clock := newTestEngine(t, closeOnComplete(0), WithResolver(twoSlabResolver()))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		slabRaw(slabA, testTime.Add(12*time.Second)),
		slabRaw(slabA, testTime.Add(13*time.Second)), // same slab twice still isn't every slab
	)
	advanceAndSweep(t, eng, clock, 30*time.Second)
	expectNoEnvelope(t, eng, 50*time.Millisecond)

	clock.Advance(30 * time.Second)
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonCloseAfter, env.Reason)
	require.Zero(t, eng.Stats().ClosedComplete)
}

func TestCloseOnComplete_GraceCatchesLateRecords(t *testing.T) {
	eng, clock := newTestEngine(t, closeOnComplete(2*time.Second), WithResolver(twoSlabResolver()))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		slabRaw(slabA, testTime.Add(12*time.Second)),
		slabRaw(slabB, testTime.Add(12*time.Second)),
	)
	advanceAndSweep(t, eng, clock, time.Second)

	// A magrtrsrv log arriving after the slab logs, within the grace period.
	submitAll(t, eng, RawLog{Line: magrtrsrvLine(slabA, 4, mcast), Time: testTime.Add(13 * time.Second)})

	clock.Advance(time.Second)
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonComplete, env.Reason)
	require.Equal(t, 1, env.SourceCounts()["magrtrsrv"])
}

func TestCloseOnComplete_NoSlabListNeverCompletes(t *testing.T) {
	eng, clock := newTestEngine(t, closeOnComplete(0)) // nop resolver: no slab list

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	advanceAndSweep(t, eng, clock, 30*time.Second)
	expectNoEnvelope(t, eng, 50*time.Millisecond)

	clock.Advance(30 * time.Second)
	require.Equal(t, ReasonCloseAfter, recvEnvelope(t, eng).Reason)
}

func TestCloseOnComplete_PartialEnvelopeCompletesToo(t *testing.T) {
	eng, clock := newTestEngine(t, closeOnComplete(0), WithResolver(twoSlabResolver()))

	submitAll(t, eng,
		RawLog{Line: magnumALine(testSrc, testDst), Time: testTime}, // no scheduler: partial
		slabRaw(slabA, testTime.Add(10*time.Second)),
		slabRaw(slabB, testTime.Add(10*time.Second)),
	)
	clock.Advance(time.Second)
	env := recvEnvelope(t, eng)
	require.Equal(t, ReasonComplete, env.Reason)
	require.True(t, env.Partial)
}

func TestCloseOnComplete_GraceDefaults(t *testing.T) {
	require.Equal(t, 2*time.Second, DefaultConfig().CompleteGrace)
	require.Equal(t, time.Duration(0), applyDefaults(Config{CompleteGrace: -time.Second}).CompleteGrace)
	require.Equal(t, time.Duration(0), applyDefaults(Config{}).CompleteGrace) // zero is a valid "next sweep"
}
