package beater

import (
	"fmt"

	"github.com/elastic/elastic-agent-libs/logp"

	"github.com/thetherington/routelogbeat/beater/correlator"
)

// correlatorDebugSelector is the logp selector for the correlator's
// per-record diagnostics. Enable with `logging.level: debug` and
// `logging.selectors: ["correlator"]` (or `-d "correlator"` on the command line).
const correlatorDebugSelector = "correlator"

// correlatorOptions returns the engine options common to production: the
// resolver and the real clock, plus — only when debug logging is enabled for
// the correlator selector — the unresolved-record observer. The observer walks
// the slab index to build each event, so it is not installed otherwise.
func correlatorOptions(resolver correlator.Resolver, metadata correlator.MetadataResolver) []correlator.Option {
	opts := []correlator.Option{
		correlator.WithResolver(resolver),
		correlator.WithMetadataResolver(metadata),
		correlator.WithClock(correlator.RealClock{}),
	}
	if logp.IsDebug(correlatorDebugSelector) {
		opts = append(opts, correlator.WithUnresolvedObserver(
			logUnresolved(logp.NewLogger(correlatorDebugSelector))))
	}
	return opts
}

// logUnresolved returns an observer that logs slab/magrtrsrv records the engine
// could not correlate: once when a record is first buffered, and once more if
// it is dropped after pending_wait (the drop line carries the final reason).
func logUnresolved(log *logp.Logger) correlator.UnresolvedObserver {
	return func(ev correlator.UnresolvedEvent) {
		msg := "record could not be correlated yet, buffered"
		if ev.Dropped {
			msg = "record dropped, never correlated"
		}

		kv := []interface{}{
			"reason", ev.Reason.String(),
			"kind", ev.Kind.String(),
			"hostname", ev.Hostname,
			"dst_num", ev.DstNum,
			"multicast", ev.Multicast,
			"record_time", ev.RecordTime,
			"waited", ev.Waited,
			"hint", unresolvedHint(ev),
		}
		switch ev.Reason {
		case correlator.MissMulticastConflict:
			kv = append(kv, "expected_multicast", ev.ExpectedMulticast)
			if ev.MatchedKey != nil {
				kv = append(kv, "matched_src", ev.MatchedKey.Src, "matched_dst", ev.MatchedKey.Dst)
			}
		case correlator.MissNoSlabMatch:
			kv = append(kv,
				"open_envelopes", ev.OpenEnvelopes,
				"open_without_slab_list", ev.OpenWithoutSlabs,
				"known_dst_nums_for_hostname", ev.KnownDstNumsForHostname,
				"known_hostnames_for_dst_num", ev.KnownHostnamesForDstNum)
		}

		log.Debugw(msg, kv...)
	}
}

// unresolvedHint turns an event into a one-line likely cause.
func unresolvedHint(ev correlator.UnresolvedEvent) string {
	switch ev.Reason {
	case correlator.MissMulticastConflict:
		return fmt.Sprintf("hostname/DST# matched an open envelope, but this log's multicast %q differs from the %q magrtrsrv reported for it",
			ev.Multicast, ev.ExpectedMulticast)

	case correlator.MissNoSlabMatch:
		switch {
		case ev.OpenEnvelopes == 0:
			return "no envelope is open: no scheduler/magnum log opened one for this route, or it already closed (close_after)"
		case len(ev.KnownDstNumsForHostname) > 0:
			return fmt.Sprintf("hostname is known but under DST# %v, not %d: the slab cache and the log disagree on the DST number",
				ev.KnownDstNumsForHostname, ev.DstNum)
		case len(ev.KnownHostnamesForDstNum) > 0:
			return fmt.Sprintf("DST# %d is known but under hostname(s) %v, not %q: the slab cache and the log disagree on the hostname",
				ev.DstNum, ev.KnownHostnamesForDstNum, ev.Hostname)
		case ev.OpenWithoutSlabs > 0:
			return fmt.Sprintf("%d of %d open envelope(s) have no slab list: the resolver returned nothing for their destination (is the slab cache populated for it?)",
				ev.OpenWithoutSlabs, ev.OpenEnvelopes)
		default:
			return "neither this hostname nor DST# appears in any open envelope's slab list: this slab belongs to a different destination than any route currently in flight"
		}
	}
	return ""
}
