package beater

import (
	"sort"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/elastic-agent-libs/mapstr"

	"github.com/thetherington/routelogbeat/beater/correlator"
)

// matchedLogRank orders matchedLogs by log type; within a type, logs are
// ordered by time.
var matchedLogRank = map[correlator.Kind]int{
	correlator.KindScheduler:       0,
	correlator.KindMagclientsrv:    1,
	correlator.KindMagnumSubscribe: 2,
	correlator.KindMagnumComplete:  3,
	correlator.KindMagrtrsrv:       4,
	correlator.KindSlab:            5,
}

// envelopeToEvent builds the Elasticsearch document for one closed envelope.
// Values that are unknown (no metadata, no magclientsrv log, no slab log, ...)
// are left out of the document rather than set to "" or 0.
func envelopeToEvent(env *correlator.Envelope) beat.Event {
	fields := mapstr.M{
		"destinationId": env.Key.Dst,
		"sourceId":      env.Key.Src,
		"matchedLogs":   matchedLogs(env.Records),
		"complete":      isComplete(env.Records),
		"partial":       env.Partial,
	}

	putString(fields, "destinationName", env.Resolved.DstMeta["name"])
	putString(fields, "destinationLabel", env.Resolved.DstMeta["label"])
	putString(fields, "destinationTag", env.Resolved.DstMeta["tag"])
	putString(fields, "sourceName", env.Resolved.SrcMeta["name"])
	putString(fields, "sourceLabel", env.Resolved.SrcMeta["label"])
	putString(fields, "clientIP", env.Resolved.ClientIP)
	putString(fields, "multicastAddress", env.Resolved.Multicast)

	if ms, ok := env.SchedulerToSlabMillis(); ok {
		fields["durationMs"] = ms
	}
	if slabs := loggedSlabs(env.Records); len(slabs) > 0 {
		fields["slabs"] = slabs
	}

	return beat.Event{
		Timestamp: eventTime(env),
		Fields:    fields,
	}
}

// eventTime is the time of the first scheduler, magclientsrv or magnum
// Subscribe request log, falling back to the envelope's first log of any kind.
func eventTime(env *correlator.Envelope) time.Time {
	var first time.Time
	for _, r := range env.Records {
		switch r.Kind {
		case correlator.KindScheduler, correlator.KindMagclientsrv, correlator.KindMagnumSubscribe:
			if first.IsZero() || r.Time.Before(first) {
				first = r.Time
			}
		}
	}
	if first.IsZero() {
		return env.OpenedAt
	}
	return first
}

// matchedLogs lists every correlated log in type order, then time order. It
// sorts a copy, so env.Records keeps its arrival order.
func matchedLogs(records []correlator.Record) []mapstr.M {
	sorted := make([]correlator.Record, len(records))
	copy(sorted, records)
	sort.SliceStable(sorted, func(i, j int) bool {
		ri, rj := matchedLogRank[sorted[i].Kind], matchedLogRank[sorted[j].Kind]
		if ri != rj {
			return ri < rj
		}
		return sorted[i].Time.Before(sorted[j].Time)
	})

	logs := make([]mapstr.M, 0, len(sorted))
	for _, r := range sorted {
		logs = append(logs, mapstr.M{
			"log":  r.Raw,
			"time": r.Time,
			"type": r.Kind.String(),
		})
	}
	return logs
}

// isComplete reports whether the route reached the slabs: a magrtrsrv or slab
// log was correlated. A magrtrsrv log alone is enough, since slab logs are
// often missing for technical reasons.
func isComplete(records []correlator.Record) bool {
	for _, r := range records {
		if r.Kind == correlator.KindMagrtrsrv || r.Kind == correlator.KindSlab {
			return true
		}
	}
	return false
}

// loggedSlabs lists each slab that sent a slab log, once, ordered by the time
// of its first slab log (the same order the slabs appear in matchedLogs).
func loggedSlabs(records []correlator.Record) []mapstr.M {
	first := map[correlator.SlabRef]time.Time{}
	var refs []correlator.SlabRef
	for _, r := range records {
		if r.Kind != correlator.KindSlab || r.Slab == nil {
			continue
		}
		ref := correlator.SlabRef{Hostname: r.Slab.Hostname, DstNum: r.Slab.DstNum}
		t, seen := first[ref]
		if !seen {
			refs = append(refs, ref)
		}
		if !seen || r.Time.Before(t) {
			first[ref] = r.Time
		}
	}
	sort.SliceStable(refs, func(i, j int) bool { return first[refs[i]].Before(first[refs[j]]) })

	slabs := make([]mapstr.M, 0, len(refs))
	for _, ref := range refs {
		slabs = append(slabs, mapstr.M{
			"slabName":   ref.Hostname,
			"slabOutput": ref.DstNum,
		})
	}
	return slabs
}

// putString sets fields[key] to v when v is a non-empty string.
func putString(fields mapstr.M, key string, v any) {
	if s, ok := v.(string); ok && s != "" {
		fields[key] = s
	}
}
