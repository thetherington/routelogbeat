package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testSrc = "3fd8c558-c7dd-5ca4-8cb1-3f5d2a525c6f"
	testDst = "d113cd1e-ad09-5e1e-a278-21ff6914f9ce"
)

var testTime = time.Date(2026, 8, 23, 4, 0, 0, 65_000_000, time.UTC)

func TestSchedulerParser(t *testing.T) {
	p := schedulerParser{}
	require.Equal(t, "scheduler", p.Name())
	require.Equal(t, KindScheduler, p.Kind())

	t.Run("v1 happy path", func(t *testing.T) {
		line := `main: Sending route: [[{'src': ['` + testSrc + `'], 'dst': ['` + testDst + `']}]]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindScheduler, recs[0].Kind)
		require.Equal(t, &Key{Src: testSrc, Dst: testDst}, recs[0].Key)
		require.Equal(t, "sending_route", recs[0].Fields["scheduler_variant"])
	})

	t.Run("v1 uppercase uuid lower-cased", func(t *testing.T) {
		upper := `main: Sending route: [[{'src': ['3FD8C558-C7DD-5CA4-8CB1-3F5D2A525C6F'], 'dst': ['` + testDst + `']}]]`
		recs, err := p.Parse(RawLog{Line: upper, Time: testTime})
		require.NoError(t, err)
		require.Equal(t, testSrc, recs[0].Key.Src)
	})

	t.Run("v2 dcpipes happy path", func(t *testing.T) {
		line := `dcpipes.jsonrpctcp: SENDING: {'params': [[{'src': ['` + testSrc + `'], 'dst': ['` + testDst + `']}]], 'method': 'route'}`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, &Key{Src: testSrc, Dst: testDst}, recs[0].Key)
		require.Equal(t, "dcpipes", recs[0].Fields["scheduler_variant"])
	})

	t.Run("multiple route objects in one line", func(t *testing.T) {
		src2, dst2 := "44444444-4444-4444-4444-444444444444", "55555555-5555-5555-5555-555555555555"
		line := `main: Sending route: [[{'src': ['` + testSrc + `'], 'dst': ['` + testDst + `']}, {'src': ['` + src2 + `'], 'dst': ['` + dst2 + `']}]]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 2)
		require.Equal(t, &Key{Src: testSrc, Dst: testDst}, recs[0].Key)
		require.Equal(t, &Key{Src: src2, Dst: dst2}, recs[1].Key)
	})

	t.Run("multi-entry src list is a parse error", func(t *testing.T) {
		other := "44444444-4444-4444-4444-444444444444"
		line := `main: Sending route: [[{'src': ['` + testSrc + `', '` + other + `'], 'dst': ['` + testDst + `']}]]`
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("unrelated line does not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "some unrelated log line", Time: testTime})
		require.ErrorIs(t, err, ErrNoMatch)
	})

	t.Run("prefilter hit but gate miss", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "Sending route: but not the expected shape at all", Time: testTime})
		require.ErrorIs(t, err, ErrNoMatch)
	})
}

func TestMagnumParser(t *testing.T) {
	p := magnumParser{}
	require.Equal(t, "magnum", p.Name())
	require.Equal(t, KindMagnumSubscribe, p.Kind())

	t.Run("variant A happy path", func(t *testing.T) {
		line := `INFO:jsonrpc:Subscribe request. Dst [('` + testDst + `',)], Sub [('` + testSrc + `',)], User [None], ID [None]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindMagnumSubscribe, recs[0].Kind)
		require.Equal(t, &Key{Src: testSrc, Dst: testDst}, recs[0].Key)
		require.Equal(t, "subscribe_request", recs[0].Fields["magnum_variant"])
		require.Equal(t, "dst_sub", recs[0].Fields["subscribe_shape"])
	})

	t.Run("variant A multi-entry list is a parse error", func(t *testing.T) {
		other := "44444444-4444-4444-4444-444444444444"
		line := `INFO:jsonrpc:Subscribe request. Dst [('` + testDst + `',), ('` + other + `',)], Sub [('` + testSrc + `',)]`
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("variant B happy path", func(t *testing.T) {
		line := `INFO:subscription:Subscription Request Complete: Routes [1-1]: [{'dst': ['` + testDst + `'], 'sub_dst': ['` + testSrc + `']}]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindMagnumComplete, recs[0].Kind)
		require.Equal(t, &Key{Src: testSrc, Dst: testDst}, recs[0].Key)
		require.Equal(t, "request_complete", recs[0].Fields["magnum_variant"])
		require.Equal(t, "1-1", recs[0].Fields["routes"])
	})

	t.Run("variant B multiple objects in one line", func(t *testing.T) {
		src2, dst2 := "44444444-4444-4444-4444-444444444444", "55555555-5555-5555-5555-555555555555"
		line := `INFO:subscription:Subscription Request Complete: Routes [1-2]: [{'dst': ['` + testDst + `'], 'sub_dst': ['` + testSrc + `']}, {'dst': ['` + dst2 + `'], 'sub_dst': ['` + src2 + `']}]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 2)
	})

	t.Run("unrelated line does not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "some unrelated log line", Time: testTime})
		require.ErrorIs(t, err, ErrNoMatch)
	})
}

func TestSlabParser(t *testing.T) {
	p := slabParser{}
	require.Equal(t, "slab", p.Name())
	require.Equal(t, KindSlab, p.Kind())

	t.Run("happy path", func(t *testing.T) {
		line := `sv7bc-slab027 [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: username:legacy_login AuditSet:DST 4 ADDR:"239.32.111.55;sync-time=3000"` + "\n"
		recs, err := p.Parse(RawLog{Line: line, Time: testTime, Hostname: "sv7bc-slab027"})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindSlab, recs[0].Kind)
		require.Equal(t, &SlabObs{Hostname: "sv7bc-slab027", DstNum: 4, Multicast: "239.32.111.55"}, recs[0].Slab)
		require.Nil(t, recs[0].Key) // resolved by the engine, not the parser

		lt, ok := recs[0].Fields["line_time"].(time.Time)
		require.True(t, ok)
		require.Equal(t, 2026, lt.Year())
		require.Equal(t, time.August, lt.Month())
		require.Equal(t, 23, lt.Day())

		// RawLog.Time, not the in-line timestamp, is what correlation uses.
		require.Equal(t, testTime, recs[0].Time)
	})

	t.Run("hostname echoes RawLog.Hostname verbatim, not derived from Line", func(t *testing.T) {
		line := `sv7bc-slab027 [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: AuditSet:DST 4 ADDR:"239.32.111.55"`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime, Hostname: "some-other-host"})
		require.NoError(t, err)
		require.Equal(t, "some-other-host", recs[0].Slab.Hostname)
	})

	t.Run("missing ADDR still resolves via DST number", func(t *testing.T) {
		line := `sv7bc-slab058 [23.08.2026 04:00:12.653] W: exlwrp-lwrp: info: AuditSet:DST 4`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime, Hostname: "sv7bc-slab058"})
		require.NoError(t, err)
		require.Equal(t, "", recs[0].Slab.Multicast)
		require.Equal(t, 4, recs[0].Slab.DstNum)
	})

	t.Run("empty hostname is a parse error", func(t *testing.T) {
		line := `sv7bc-slab027 [23.08.2026 04:00:12.641] W: exlwrp-lwrp: info: AuditSet:DST 4 ADDR:"239.32.111.55"`
		_, err := p.Parse(RawLog{Line: line, Time: testTime, Hostname: ""})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("unrelated line does not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "some unrelated log line", Time: testTime, Hostname: "h"})
		require.ErrorIs(t, err, ErrNoMatch)
	})
}

func TestMagrtrsrvParser(t *testing.T) {
	p := magrtrsrvParser{}
	require.Equal(t, "magrtrsrv", p.Name())
	require.Equal(t, KindMagrtrsrv, p.Kind())

	t.Run("happy path", func(t *testing.T) {
		line := `INFO:commands:Cmd. D [1051], N [sv7bc-slab058], M [set.rx.route], A [[[{'dest': {'output': 4, 'port_type': 1, 'stream_type': 2}, 'sources': [{'sfp': 1, 'multicast_ip': '239.32.111.55', 'udp_port': 5004, 'source_ips': ['10.12.42.89']}]}]]], K [{}]`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindMagrtrsrv, recs[0].Kind)
		require.Equal(t, &SlabObs{Hostname: "sv7bc-slab058", DstNum: 4, Multicast: "239.32.111.55"}, recs[0].Slab)
		require.Nil(t, recs[0].Key)
		require.Equal(t, 5004, recs[0].Fields["udp_port"])
		require.Equal(t, "'10.12.42.89'", recs[0].Fields["source_ips"])
	})

	t.Run("missing multicast_ip is a parse error", func(t *testing.T) {
		line := `INFO:commands:Cmd. D [1051], N [sv7bc-slab058], M [set.rx.route], A [[[{'dest': {'output': 4}, 'sources': [{'sfp': 1, 'udp_port': 5004}]}]]], K [{}]`
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("missing output is a parse error", func(t *testing.T) {
		line := `INFO:commands:Cmd. D [1051], N [sv7bc-slab058], M [set.rx.route], A [[[{'dest': {}, 'sources': [{'multicast_ip': '239.32.111.55'}]}]]], K [{}]`
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("unrelated line does not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "some unrelated log line", Time: testTime})
		require.ErrorIs(t, err, ErrNoMatch)
	})
}

// The second "Subscribe request." shape carries a Subscription [{'dst': [...],
// 'sub_dst': [...]}] route object instead of Dst [...] / Sub [...] fields.
func TestMagnumParser_SubscribeSubscriptionShape(t *testing.T) {
	p := magnumParser{}

	const (
		dst = "af2925a1-a72f-5355-a5d5-c3ec7f5de929"
		sub = "d92a097a-9ee7-54ab-b255-7c101ec80b93"
		id  = "0299cc2e-4ffe-4acd-8c0d-570da767a37f"
	)

	t.Run("real sample line", func(t *testing.T) {
		line := "INFO:jsonrpc:Subscribe request. Subscription [{'dst': ['" + dst + "'], 'sub_dst': ['" + sub + "']}], User [admin], ID [" + id + "]"
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindMagnumSubscribe, recs[0].Kind)
		require.Equal(t, &Key{Src: sub, Dst: dst}, recs[0].Key) // Src is sub_dst; the ID [...] UUID is not part of the key
		require.Equal(t, "subscribe_request", recs[0].Fields["magnum_variant"])
		require.Equal(t, "subscription", recs[0].Fields["subscribe_shape"])
	})

	t.Run("both shapes of the same route yield the same key", func(t *testing.T) {
		shape1 := "INFO:jsonrpc:Subscribe request. Dst [('" + dst + "',)], Sub [('" + sub + "',)], User [None], ID [None]"
		shape2 := "INFO:jsonrpc:Subscribe request. Subscription [{'dst': ['" + dst + "'], 'sub_dst': ['" + sub + "']}], User [admin], ID [" + id + "]"

		r1, err := p.Parse(RawLog{Line: shape1, Time: testTime})
		require.NoError(t, err)
		r2, err := p.Parse(RawLog{Line: shape2, Time: testTime})
		require.NoError(t, err)
		require.Equal(t, r1[0].Key, r2[0].Key)
	})

	t.Run("multiple route objects yield one record each", func(t *testing.T) {
		dst2, sub2 := "55555555-5555-5555-5555-555555555555", "44444444-4444-4444-4444-444444444444"
		line := "INFO:jsonrpc:Subscribe request. Subscription [{'dst': ['" + dst + "'], 'sub_dst': ['" + sub + "']}, {'dst': ['" + dst2 + "'], 'sub_dst': ['" + sub2 + "']}], User [admin], ID [" + id + "]"
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 2)
		require.Equal(t, &Key{Src: sub, Dst: dst}, recs[0].Key)
		require.Equal(t, &Key{Src: sub2, Dst: dst2}, recs[1].Key)
		recs[0].Fields["x"] = 1 // records must not share a Fields map
		require.NotContains(t, recs[1].Fields, "x")
	})

	t.Run("multi-entry dst list is a parse error", func(t *testing.T) {
		other := "44444444-4444-4444-4444-444444444444"
		line := "INFO:jsonrpc:Subscribe request. Subscription [{'dst': ['" + dst + "', '" + other + "'], 'sub_dst': ['" + sub + "']}], User [admin], ID [" + id + "]"
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})

	t.Run("no route object is a parse error", func(t *testing.T) {
		line := "INFO:jsonrpc:Subscribe request. Subscription [], User [admin], ID [" + id + "]"
		_, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.Error(t, err)
		require.NotEqual(t, ErrNoMatch, err)
	})
}
