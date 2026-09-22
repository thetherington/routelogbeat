package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEndToEnd_UserSnippet replays the log lines from the design discussion
// — one scheduler, two magnum, two slab, optionally one magrtrsrv — through
// a real Engine with RealClock, and checks the resulting envelope end to
// end. This is the plan's Verification step 4, split into the two cases
// that matter for Resolved.Multicast: without a magrtrsrv log (the slab
// logs' own ADDR field is the only multicast source) and with one (it
// always wins, even over a slab log that reports something different).
func TestEndToEnd_UserSnippet(t *testing.T) {
	const (
		src = "3fd8c558-c7dd-5ca4-8cb1-3f5d2a525c6f"
		dst = "d113cd1e-ad09-5e1e-a278-21ff6914f9ce"
	)
	parseTime := func(t *testing.T, s string) time.Time {
		t.Helper()
		ts, err := time.Parse(time.RFC3339Nano, s)
		require.NoError(t, err)
		return ts
	}

	newEngine := func(t *testing.T) *Engine {
		t.Helper()
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
		t.Cleanup(eng.Close)
		return eng
	}

	// schedulerAndMagnum are the first three logs, identical in both cases.
	schedulerAndMagnum := func(t *testing.T) []RawLog {
		return []RawLog{
			{
				Line: `main: Sending route: [[{'src': ['` + src + `'], 'dst': ['` + dst + `']}]]`,
				Time: parseTime(t, "2026-08-23T04:00:00.065Z"),
			},
			{
				Line: `INFO:jsonrpc:Subscribe request. Dst [('` + dst + `',)], Sub [('` + src + `',)], User [None], ID [None]`,
				Time: parseTime(t, "2026-08-23T04:00:02.506Z"),
			},
			{
				Line: `INFO:subscription:Subscription Request Complete: Routes [1-1]: [{'dst': ['` + dst + `'], 'sub_dst': ['` + src + `']}]`,
				Time: parseTime(t, "2026-08-23T04:00:02.509Z"),
			},
		}
	}

	// slabLogs carries its own ADDR value so both subtests can use a
	// different one — the "without magrtrsrv" case needs this to actually be
	// the multicast the engine reports; the "with" case needs it to differ
	// from magrtrsrv's so the override assertion proves something.
	slabLogs := func(t *testing.T, addr string) []RawLog {
		return []RawLog{
			{
				Line:     `sv7bc-slab027 [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: username:legacy_login AuditSet:DST 4 ADDR:"` + addr + `;sync-time=3000"` + "\n",
				Time:     parseTime(t, "2026-08-23T04:00:12.641Z"),
				Hostname: "sv7bc-slab027",
			},
			{
				Line:     `sv7bc-slab058 [23.08.2026 04:00:12.652] W: exlwrp-lwrp: info: username:legacy_login AuditSet:DST 4 ADDR:"` + addr + `;sync-time=3000"` + "\n",
				Time:     parseTime(t, "2026-08-23T04:00:12.653Z"),
				Hostname: "sv7bc-slab058",
			},
		}
	}

	submitAndReceive := func(t *testing.T, eng *Engine, logs []RawLog) *Envelope {
		t.Helper()
		for _, l := range logs {
			require.True(t, eng.Submit(l))
		}
		select {
		case env := <-eng.Events():
			return env
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for the envelope")
			return nil
		}
	}

	// Common assertions that hold regardless of whether magrtrsrv showed up:
	// correlation identity, who opened it, that it closed on schedule, and
	// the two duration metrics — SchedulerToSlabMillis in particular ignores
	// magrtrsrv/magnum entirely, so it must read the same in both cases.
	assertCommon := func(t *testing.T, env *Envelope) {
		t.Helper()
		require.Equal(t, Key{Src: src, Dst: dst}, env.Key)
		require.Equal(t, KindScheduler, env.OpenedBy)
		require.False(t, env.Partial)
		require.Equal(t, ReasonCloseAfter, env.Reason)

		wantDuration := parseTime(t, "2026-08-23T04:00:12.653Z").Sub(parseTime(t, "2026-08-23T04:00:00.065Z"))
		require.Equal(t, wantDuration, env.Duration())
		require.Equal(t, 12_588*time.Millisecond, env.Duration())

		ms, ok := env.SchedulerToSlabMillis()
		require.True(t, ok)
		require.EqualValues(t, 12_588, ms)
	}

	t.Run("without magrtrsrv: multicast comes from the slab logs' own ADDR field", func(t *testing.T) {
		eng := newEngine(t)
		logs := append(schedulerAndMagnum(t), slabLogs(t, "239.32.111.55")...)

		env := submitAndReceive(t, eng, logs)
		assertCommon(t, env)

		require.Len(t, env.Records, 5)
		require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "slab": 2}, env.SourceCounts())
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // the only multicast source available
	})

	t.Run("with magrtrsrv: its multicast overwrites the slab logs' own ADDR value", func(t *testing.T) {
		eng := newEngine(t)

		magrtrsrv := RawLog{
			Line: `INFO:commands:Cmd. D [1051], N [sv7bc-slab027], M [set.rx.route], A [[[{'dest': {'output': 4, 'port_type': 1, 'stream_type': 2}, 'sources': [{'sfp': 1, 'multicast_ip': '239.32.111.55', 'udp_port': 5004, 'source_ips': ['10.12.42.89']}]}]]], K [{}]`,
			// Arrives before its slab logs, as the design assumes (though
			// resolution doesn't depend on that order — see correlator_test.go).
			Time: parseTime(t, "2026-08-23T04:00:12.600Z"),
		}
		logs := append(schedulerAndMagnum(t), magrtrsrv)
		// A deliberately different ADDR from magrtrsrv's multicast_ip, so the
		// assertion below actually proves precedence rather than agreeing by
		// coincidence.
		logs = append(logs, slabLogs(t, "239.32.111.99")...)

		env := submitAndReceive(t, eng, logs)
		assertCommon(t, env)

		require.Len(t, env.Records, 6)
		require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "magrtrsrv": 1, "slab": 2}, env.SourceCounts())
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // magrtrsrv's, not the slab logs' "239.32.111.99"
	})
}
