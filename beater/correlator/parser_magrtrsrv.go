package correlator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	magrtrsrvHostRe   = regexp.MustCompile(`N\s*\[([^\]]+)\]`)
	magrtrsrvOutputRe = regexp.MustCompile(`'output'\s*:\s*(\d+)`)
	magrtrsrvMcastRe  = regexp.MustCompile(`'multicast_ip'\s*:\s*'([0-9]{1,3}(?:\.[0-9]{1,3}){3})'`)
	magrtrsrvPortRe   = regexp.MustCompile(`'udp_port'\s*:\s*(\d+)`)
	magrtrsrvSrcIPsRe = regexp.MustCompile(`'source_ips'\s*:\s*\[([^\]]*)\]`)
)

// magrtrsrvParser recognizes "M [set.rx.route]" command lines from the
// magrtrsrv process — the only reliable source of a multicast address the
// engine ever sees. It never opens an envelope; it resolves via the exact
// same hostname+DST# mechanism as slabParser (dest.output == DST <n>,
// confirmed with the user). Exactly one {dest, sources} pair is expected per
// line; anything else is a parse error, not expanded into multiple records.
type magrtrsrvParser struct{}

func (magrtrsrvParser) Name() string { return "magrtrsrv" }
func (magrtrsrvParser) Kind() Kind   { return KindMagrtrsrv }

func (p magrtrsrvParser) Parse(raw RawLog) ([]Record, error) {
	if !strings.Contains(raw.Line, "M [set.rx.route]") {
		return nil, ErrNoMatch
	}

	hostM := magrtrsrvHostRe.FindStringSubmatch(raw.Line)
	if hostM == nil {
		return nil, fmt.Errorf("correlator: magrtrsrv line matched but N [...] hostname not found")
	}
	hostname := strings.TrimSpace(hostM[1])

	outputs := magrtrsrvOutputRe.FindAllStringSubmatch(raw.Line, -1)
	if len(outputs) != 1 {
		return nil, fmt.Errorf("correlator: magrtrsrv line matched but found %d 'output' fields, want 1", len(outputs))
	}
	dstNum, err := strconv.Atoi(outputs[0][1])
	if err != nil {
		return nil, fmt.Errorf("correlator: magrtrsrv output number: %w", err)
	}

	mcasts := magrtrsrvMcastRe.FindAllStringSubmatch(raw.Line, -1)
	if len(mcasts) != 1 {
		return nil, fmt.Errorf("correlator: magrtrsrv line matched but found %d 'multicast_ip' fields, want 1", len(mcasts))
	}
	multicast := mcasts[0][1]

	fields := map[string]any{}
	if m := magrtrsrvPortRe.FindStringSubmatch(raw.Line); m != nil {
		if port, err := strconv.Atoi(m[1]); err == nil {
			fields["udp_port"] = port
		}
	}
	if m := magrtrsrvSrcIPsRe.FindStringSubmatch(raw.Line); m != nil {
		fields["source_ips"] = m[1]
	}

	return []Record{{
		Kind:   KindMagrtrsrv,
		Time:   raw.Time,
		Slab:   &SlabObs{Hostname: hostname, DstNum: dstNum, Multicast: multicast},
		Fields: fields,
		Raw:    raw.Line,
	}}, nil
}
