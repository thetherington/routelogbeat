package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEndToEnd_UserSnippet replays the log lines from the design discussion
// — one scheduler, two magnum, two slab, optionally one magrtrsrv — through
// a real Engine with RealClock, and checks the resulting envelope end to
// end. This is the plan's Verification step 4, with subtests for the cases
// that matter:
//
//   - how a slab record resolves and what ends up in Resolved.Multicast:
//     without a magrtrsrv log (hostname/DST# alone is enough, and the slab
//     logs' own ADDR field is the only multicast source); with one reporting
//     the same multicast the slab logs carry (both checks pass, so they
//     still attach); and with one reporting something different (the slab
//     logs then never correlate at all — see correlator_test.go's
//     TestEngine_MagrtrsrvDictatesSlabMulticast for the engine-level
//     coverage of this rule).
//   - what happens with no scheduler log at all: magnum opens a partial
//     envelope instead, and SchedulerToSlabMillis becomes unavailable even
//     though Duration() still works fine.
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

	schedulerLog := func(t *testing.T) RawLog {
		return RawLog{
			Line: `main: Sending route: [[{'src': ['` + src + `'], 'dst': ['` + dst + `']}]]`,
			Time: parseTime(t, "2026-08-23T04:00:00.065Z"),
		}
	}

	// magnumLogs are the two magnum lines, used both on their own (no
	// scheduler) and prefixed with schedulerLog via schedulerAndMagnum.
	magnumLogs := func(t *testing.T) []RawLog {
		return []RawLog{
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

	schedulerAndMagnum := func(t *testing.T) []RawLog {
		return append([]RawLog{schedulerLog(t)}, magnumLogs(t)...)
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

	// Common assertions that hold in every case: correlation identity, who
	// opened it, that it closed on schedule.
	assertCommon := func(t *testing.T, env *Envelope) {
		t.Helper()
		require.Equal(t, Key{Src: src, Dst: dst}, env.Key)
		require.Equal(t, KindScheduler, env.OpenedBy)
		require.False(t, env.Partial)
		require.Equal(t, ReasonCloseAfter, env.Reason)
	}

	// magrtrsrvLog arrives before its slab logs, as the design assumes
	// (though resolution doesn't depend on that order — see
	// correlator_test.go's TestEngine_MagrtrsrvDictatesSlabMulticast).
	magrtrsrvLog := func(t *testing.T, multicastIP string) RawLog {
		return RawLog{
			Line: `INFO:commands:Cmd. D [1051], N [sv7bc-slab027], M [set.rx.route], A [[[{'dest': {'output': 4, 'port_type': 1, 'stream_type': 2}, 'sources': [{'sfp': 1, 'multicast_ip': '` + multicastIP + `', 'udp_port': 5004, 'source_ips': ['10.12.42.89']}]}]]], K [{}]`,
			Time: parseTime(t, "2026-08-23T04:00:12.600Z"),
		}
	}

	t.Run("without magrtrsrv: hostname/DST# alone is enough, multicast comes from the slab logs' own ADDR field", func(t *testing.T) {
		eng := newEngine(t)
		logs := append(schedulerAndMagnum(t), slabLogs(t, "239.32.111.55")...)

		env := submitAndReceive(t, eng, logs)
		assertCommon(t, env)

		require.Len(t, env.Records, 5)
		require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "slab": 2}, env.SourceCounts())
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // the only multicast source available

		wantDuration := parseTime(t, "2026-08-23T04:00:12.653Z").Sub(parseTime(t, "2026-08-23T04:00:00.065Z"))
		require.Equal(t, wantDuration, env.Duration())
		require.Equal(t, 12_588*time.Millisecond, env.Duration())

		ms, ok := env.SchedulerToSlabMillis()
		require.True(t, ok)
		require.EqualValues(t, 12_588, ms)
	})

	t.Run("with magrtrsrv reporting the same multicast: hostname/DST# plus multicast both match, slab logs still attach", func(t *testing.T) {
		eng := newEngine(t)
		logs := append(schedulerAndMagnum(t), magrtrsrvLog(t, "239.32.111.55"))
		logs = append(logs, slabLogs(t, "239.32.111.55")...)

		env := submitAndReceive(t, eng, logs)
		assertCommon(t, env)

		require.Len(t, env.Records, 6)
		require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "magrtrsrv": 1, "slab": 2}, env.SourceCounts())
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast)

		ms, ok := env.SchedulerToSlabMillis()
		require.True(t, ok)
		require.EqualValues(t, 12_588, ms) // unaffected by the magrtrsrv record — it only counts scheduler and slab
	})

	t.Run("with magrtrsrv reporting a different multicast: the slab logs never correlate at all", func(t *testing.T) {
		eng := newEngine(t)
		logs := append(schedulerAndMagnum(t), magrtrsrvLog(t, "239.32.111.55"))
		// A deliberately different ADDR from magrtrsrv's multicast_ip: hostname
		// and DST# still match, but that's no longer sufficient once magrtrsrv
		// has reported a multicast for this destination.
		logs = append(logs, slabLogs(t, "239.32.111.99")...)

		env := submitAndReceive(t, eng, logs)
		assertCommon(t, env)

		require.Len(t, env.Records, 4) // scheduler + 2 magnum + magrtrsrv; neither slab log attached
		require.Equal(t, map[string]int{"scheduler": 1, "magnum": 2, "magrtrsrv": 1}, env.SourceCounts())
		require.Equal(t, "239.32.111.55", env.Resolved.Multicast) // magrtrsrv's value, untouched by the rejected slab logs

		_, ok := env.SchedulerToSlabMillis()
		require.False(t, ok) // no slab record attached at all
	})

	t.Run("without scheduler: magnum opens a partial envelope, SchedulerToSlabMillis is unavailable", func(t *testing.T) {
		eng := newEngine(t)
		logs := append(magnumLogs(t), slabLogs(t, "239.32.111.55")...)

		env := submitAndReceive(t, eng, logs)

		require.Equal(t, Key{Src: src, Dst: dst}, env.Key)
		require.Equal(t, KindMagnumSubscribe, env.OpenedBy) // "Subscribe request.", the first of the two magnum lines
		require.True(t, env.Partial)                        // the configured opener (scheduler) never arrived
		require.Equal(t, ReasonCloseAfter, env.Reason)

		require.Len(t, env.Records, 4) // 2 magnum + 2 slab; no scheduler record at all
		require.Equal(t, map[string]int{"magnum": 2, "slab": 2}, env.SourceCounts())

		// Duration still spans first-to-last correlated log — just anchored on
		// magnum's time instead of scheduler's, since there is no scheduler
		// record here at all.
		wantDuration := parseTime(t, "2026-08-23T04:00:12.653Z").Sub(parseTime(t, "2026-08-23T04:00:02.506Z"))
		require.Equal(t, wantDuration, env.Duration())

		// SchedulerToSlabMillis specifically requires a scheduler record, so
		// it's unavailable here even though Duration() above is perfectly
		// well-defined — this is the divergence discussed for this method.
		_, ok := env.SchedulerToSlabMillis()
		require.False(t, ok)
	})
}
