package beater

import (
	"testing"
	"time"

	"github.com/elastic/elastic-agent-libs/mapstr"
	"github.com/stretchr/testify/require"

	"github.com/thetherington/routelogbeat/beater/correlator"
)

const (
	evSrc = "78fa211c-1e6c-5e5c-b697-1a4a7cbf5cb3"
	evDst = "af2925a1-a72f-5355-a5d5-c3ec7f5de929"
)

var evT0 = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return evT0.Add(time.Duration(ms) * time.Millisecond) }

func rec(kind correlator.Kind, ms int, raw string) correlator.Record {
	return correlator.Record{Kind: kind, Time: at(ms), Raw: raw}
}

func slabRec(kind correlator.Kind, ms int, host string, out int) correlator.Record {
	r := rec(kind, ms, kind.String()+" "+host)
	r.Slab = &correlator.SlabObs{Hostname: host, DstNum: out, Multicast: "239.131.1.163"}
	return r
}

// fullEnvelope has every log kind, with Records in a deliberately mixed
// arrival order.
func fullEnvelope() *correlator.Envelope {
	return &correlator.Envelope{
		Key:      correlator.Key{Src: evSrc, Dst: evDst},
		OpenedBy: correlator.KindScheduler,
		OpenedAt: at(0),
		LastAt:   at(12588),
		Records: []correlator.Record{
			rec(correlator.KindMagnumComplete, 2444, "complete"),
			slabRec(correlator.KindSlab, 12588, "iad1bc-slab015", 32),
			rec(correlator.KindScheduler, 0, "scheduler"),
			slabRec(correlator.KindMagrtrsrv, 12535, "iad1bc-slab034", 32),
			rec(correlator.KindMagnumSubscribe, 2441, "subscribe"),
			slabRec(correlator.KindSlab, 12576, "iad1bc-slab034", 32),
			rec(correlator.KindMagclientsrv, 4, "magclientsrv"),
		},
		Resolved: correlator.ResolvedAttrs{
			Multicast:  "239.131.1.163",
			ClientIP:   "10.103.40.46",
			ClientPort: 45662,
			DstMeta:    map[string]any{"id": evDst, "name": "DST-1", "label": "Dest One", "tag": "IPAN"},
			SrcMeta:    map[string]any{"id": evSrc, "name": "SRC-1", "label": "Source One", "tag": "SOURCE"},
		},
		Reason: correlator.ReasonCloseAfter,
	}
}

func logTypes(t *testing.T, fields mapstr.M) []string {
	t.Helper()
	var types []string
	for _, l := range fields["matchedLogs"].([]mapstr.M) {
		types = append(types, l["type"].(string))
	}
	return types
}

func TestEnvelopeToEvent_Full(t *testing.T) {
	ev := envelopeToEvent(fullEnvelope())

	require.Equal(t, at(0), ev.Timestamp) // the scheduler log
	f := ev.Fields
	require.Equal(t, evDst, f["destinationId"])
	require.Equal(t, "DST-1", f["destinationName"])
	require.Equal(t, "Dest One", f["destinationLabel"])
	require.Equal(t, "IPAN", f["destinationTag"])
	require.Equal(t, evSrc, f["sourceId"])
	require.Equal(t, "SRC-1", f["sourceName"])
	require.Equal(t, "Source One", f["sourceLabel"])
	require.Equal(t, "10.103.40.46", f["clientIP"])
	require.EqualValues(t, 12588, f["durationMs"])
	require.Equal(t, "239.131.1.163", f["multicastAddress"])
	require.Equal(t, true, f["complete"])
	require.Equal(t, false, f["partial"])
	require.Equal(t, []mapstr.M{
		{"slabName": "iad1bc-slab034", "slabOutput": 32}, // logged at 12.576s
		{"slabName": "iad1bc-slab015", "slabOutput": 32}, // logged at 12.588s, though it arrived first
	}, f["slabs"])

	// Exactly the requested fields: 4 destination, 3 source, clientIP,
	// durationMs, multicastAddress, slabs, matchedLogs, complete, partial.
	require.Len(t, f, 14)
}

func TestEnvelopeToEvent_MatchedLogsOrder(t *testing.T) {
	env := fullEnvelope()
	// A second subscribe request earlier than the first, and slabs out of order.
	env.Records = append(env.Records, rec(correlator.KindMagnumSubscribe, 2000, "subscribe early"))
	before := append([]correlator.Record(nil), env.Records...)

	logs := envelopeToEvent(env).Fields["matchedLogs"].([]mapstr.M)

	require.Equal(t, []string{
		"scheduler", "magclientsrv", "magnum_subscribe", "magnum_subscribe",
		"magnum_complete", "magrtrsrv", "slab", "slab",
	}, logTypes(t, mapstr.M{"matchedLogs": logs}))
	require.Equal(t, "subscribe early", logs[2]["log"])
	require.Equal(t, at(2000), logs[2]["time"])
	require.Equal(t, at(12576), logs[6]["time"]) // slabs by time within their type
	require.Equal(t, at(12588), logs[7]["time"])

	require.Equal(t, before, env.Records) // the envelope itself is not reordered
}

func TestEnvelopeToEvent_Timestamp(t *testing.T) {
	t.Run("magclientsrv when there is no scheduler", func(t *testing.T) {
		env := fullEnvelope()
		env.Records = []correlator.Record{
			rec(correlator.KindMagnumSubscribe, 2441, "subscribe"),
			rec(correlator.KindMagclientsrv, 4, "magclientsrv"),
		}
		require.Equal(t, at(4), envelopeToEvent(env).Timestamp)
	})

	t.Run("subscribe request when it is the only opener", func(t *testing.T) {
		env := fullEnvelope()
		env.Records = []correlator.Record{
			rec(correlator.KindMagnumComplete, 2444, "complete"),
			rec(correlator.KindMagnumSubscribe, 2441, "subscribe"),
		}
		require.Equal(t, at(2441), envelopeToEvent(env).Timestamp)
	})

	t.Run("falls back to OpenedAt with no opener log", func(t *testing.T) {
		env := fullEnvelope()
		env.OpenedAt = at(2444)
		env.Records = []correlator.Record{rec(correlator.KindMagnumComplete, 2444, "complete")}
		require.Equal(t, at(2444), envelopeToEvent(env).Timestamp)
	})
}

func TestEnvelopeToEvent_OmitsUnknownValues(t *testing.T) {
	env := &correlator.Envelope{
		Key:      correlator.Key{Src: evSrc, Dst: evDst},
		OpenedBy: correlator.KindMagnumSubscribe,
		Partial:  true,
		OpenedAt: at(0),
		Records:  []correlator.Record{rec(correlator.KindMagnumSubscribe, 0, "subscribe")},
	}
	f := envelopeToEvent(env).Fields

	require.Equal(t, mapstr.M{
		"destinationId": evDst,
		"sourceId":      evSrc,
		"complete":      false,
		"partial":       true,
		"matchedLogs":   []mapstr.M{{"log": "subscribe", "time": at(0), "type": "magnum_subscribe"}},
	}, f)
}

func TestEnvelopeToEvent_EmptyMetadataValuesOmitted(t *testing.T) {
	env := fullEnvelope()
	env.Resolved.DstMeta = map[string]any{"id": evDst, "name": "DST-1", "label": "", "tag": ""}
	f := envelopeToEvent(env).Fields

	require.Equal(t, "DST-1", f["destinationName"])
	require.NotContains(t, f, "destinationLabel")
	require.NotContains(t, f, "destinationTag")
}

func TestEnvelopeToEvent_Complete(t *testing.T) {
	base := func(records ...correlator.Record) mapstr.M {
		env := fullEnvelope()
		env.Records = append([]correlator.Record{rec(correlator.KindScheduler, 0, "scheduler")}, records...)
		return envelopeToEvent(env).Fields
	}

	t.Run("magrtrsrv without a slab log is complete", func(t *testing.T) {
		f := base(slabRec(correlator.KindMagrtrsrv, 12535, "iad1bc-slab034", 32))
		require.Equal(t, true, f["complete"])
		require.NotContains(t, f, "durationMs") // durationMs needs a slab log
		require.NotContains(t, f, "slabs")
	})

	t.Run("slab log alone is complete", func(t *testing.T) {
		f := base(slabRec(correlator.KindSlab, 12576, "iad1bc-slab034", 32))
		require.Equal(t, true, f["complete"])
		require.EqualValues(t, 12576, f["durationMs"])
	})

	t.Run("neither is not complete", func(t *testing.T) {
		f := base(rec(correlator.KindMagnumSubscribe, 2441, "subscribe"))
		require.Equal(t, false, f["complete"])
	})
}

func TestEnvelopeToEvent_SameSlabTwiceListedOnce(t *testing.T) {
	env := fullEnvelope()
	env.Records = []correlator.Record{
		rec(correlator.KindScheduler, 0, "scheduler"),
		slabRec(correlator.KindSlab, 12576, "iad1bc-slab034", 32),
		slabRec(correlator.KindSlab, 12600, "iad1bc-slab034", 32),
	}
	require.Equal(t, []mapstr.M{{"slabName": "iad1bc-slab034", "slabOutput": 32}}, envelopeToEvent(env).Fields["slabs"])
}
