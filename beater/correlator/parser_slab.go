package correlator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	slabDstNumRe = regexp.MustCompile(`AuditSet:DST\s+(\d+)`)
	slabAddrRe   = regexp.MustCompile(`ADDR:"([0-9]{1,3}(?:\.[0-9]{1,3}){3})`)
	slabTimeRe   = regexp.MustCompile(`\[([0-9]{2}\.[0-9]{2}\.[0-9]{4} [0-9:.]+)\]`)
)

// slabLineTimeLayout parses the slab line's in-line timestamp, e.g.
// "23.08.2026 04:00:12.641". It is informational only (Fields["line_time"]);
// RawLog.Time stays authoritative for correlation.
const slabLineTimeLayout = "02.01.2006 15:04:05.000"

// slabParser recognizes "AuditSet:DST" confirmation lines from the slab
// devices. Hostname comes from RawLog.Hostname (a structured field set by
// the beater from SyslogMessage.Annotation.General.DeviceName), never
// regex-extracted from the line. Never opens an envelope; resolves via
// hostname+DST# against the engine's slabIndex — and, once a magrtrsrv
// record has reported a multicast for that destination, must also carry
// that same multicast (see Engine.resolveMember in correlator.go).
type slabParser struct{}

func (slabParser) Name() string { return "slab" }
func (slabParser) Kind() Kind   { return KindSlab }

func (p slabParser) Parse(raw RawLog) ([]Record, error) {
	if !strings.Contains(raw.Line, "AuditSet:DST") {
		return nil, ErrNoMatch
	}
	if raw.Hostname == "" {
		return nil, fmt.Errorf("correlator: slab line matched but Hostname is empty")
	}

	var dstNum int
	haveDstNum := false
	if m := slabDstNumRe.FindStringSubmatch(raw.Line); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("correlator: slab DST number: %w", err)
		}
		dstNum, haveDstNum = n, true
	}

	var multicast string
	if m := slabAddrRe.FindStringSubmatch(raw.Line); m != nil {
		multicast = m[1]
	}

	if !haveDstNum && multicast == "" {
		return nil, fmt.Errorf("correlator: slab line matched but has neither DST number nor ADDR")
	}

	fields := map[string]any{}
	if m := slabTimeRe.FindStringSubmatch(raw.Line); m != nil {
		if t, err := time.Parse(slabLineTimeLayout, m[1]); err == nil {
			fields["line_time"] = t
		}
	}

	return []Record{{
		Kind:   KindSlab,
		Time:   raw.Time,
		Slab:   &SlabObs{Hostname: raw.Hostname, DstNum: dstNum, Multicast: multicast},
		Fields: fields,
		Raw:    raw.Line,
	}}, nil
}
