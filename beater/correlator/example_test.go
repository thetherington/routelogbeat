package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEndToEnd_UserSnippet replays the exact five log lines from the design
// discussion — one scheduler, two magnum, two slab — through a real Engine
// with RealClock, and checks the resulting envelope end to end. This is the
// plan's Verification step 4.
func TestEndToEnd_UserSnippet(t *testing.T) {
	const (
		src = "3fd8c558-c7dd-5ca4-8cb1-3f5d2a525c6f"
		dst = "d113cd1e-ad09-5e1e-a278-21ff6914f9ce"
	)
	parse := func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err)
		return ts
	}

	res := NewMapResolver()
	res.Store([]Entry{{
		DstUUID: dst,
		Slabs: []SlabRef{
			{Hostname: "sv7bc-slab027", DstNum: 4},
			{Hostname: "sv7bc-slab058", DstNum: 4},
		},
	}})

	eng, err := New(Config{
		OpenOn:        KindScheduler,
		CloseAfter:    50 * time.Millisecond,
		SweepInterval: 5 * time.Millisecond,
	}, WithResolver(res), WithClock(RealClock{}))
	require.NoError(t, err)
	defer eng.Close()

	logs := []RawLog{
		{
			Line: `main: Sending route: [[{'src': ['` + src + `'], 'dst': ['` + dst + `']}]]`,
			Time: parse("2026-08-23T04:00:00.065Z"),
		},
		{
			Line: `INFO:jsonrpc:Subscribe request. Dst [('` + dst + `',)], Sub [('` + src + `',)], User [None], ID [None]`,
			Time: parse("2026-08-23T04:00:02.506Z"),
		},
		{
			Line: `INFO:subscription:Subscription Request Complete: Routes [1-1]: [{'dst': ['` + dst + `'], 'sub_dst': ['` + src + `']}]`,
			Time: parse("2026-08-23T04:00:02.509Z"),
		},
		{
			Line:     `sv7bc-slab027 [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: username:legacy_login AuditSet:DST 4 ADDR:"239.32.111.55;sync-time=3000"` + "\n",
			Time:     parse("2026-08-23T04:00:12.641Z"),
			Hostname: "sv7bc-slab027",
		},
		{
			Line:     `sv7bc-slab058 [23.08.2026 04:00:12.652] W: exlwrp-lwrp: info: username:legacy_login AuditSet:DST 4 ADDR:"239.32.111.55;sync-time=3000"` + "\n",
			Time:     parse("2026-08-23T04:00:12.653Z"),
			Hostname: "sv7bc-slab058",
		},
	}
	for _, l := range logs {
		require.True(t, eng.Submit(l))
	}

	var env *Envelope
	select {
	case env = <-eng.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the envelope")
	}

	require.Equal(t, Key{Src: src, Dst: dst}, env.Key)
	require.Equal(t, KindScheduler, env.OpenedBy)
	require.False(t, env.Partial)
	require.Len(t, env.Records, 5)
	require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "slab": 2}, env.SourceCounts())
	require.Equal(t, ReasonCloseAfter, env.Reason)

	wantDuration := parse("2026-08-23T04:00:12.653Z").Sub(parse("2026-08-23T04:00:00.065Z"))
	require.Equal(t, wantDuration, env.Duration())
	require.Equal(t, 12_588*time.Millisecond, env.Duration())

	ms, ok := env.SchedulerToSlabMillis()
	require.True(t, ok)
	require.EqualValues(t, 12_588, ms) // same span here: the latest slab record is also the envelope's overall last record
}
