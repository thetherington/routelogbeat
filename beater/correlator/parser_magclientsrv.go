package correlator

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

var (
	magclientsrvGate      = regexp.MustCompile(`Method\s*\[route\]`)
	magclientsrvClientRe  = regexp.MustCompile(`Client\s*\[((?:\[[^\]]*\])?[^\]]*)\]`) // allows a bracketed IPv6 host
	magclientsrvMessageRe = regexp.MustCompile(`Message ID\s*\[(\d+)\]`)
)

// magclientsrvParser recognizes the magclientsrv dispatch log for routes:
//
//	INFO:interfaces.server:Received Dispatch Request. Client [10.103.40.46:45662], Message ID [44208], Method [route], Parameters [[[{'src': ['<uuid>'], 'dst': ['<uuid>']}]]].
//
// Only Method [route] is a match; other methods return ErrNoMatch and are
// discarded. The Parameters carry the same {'src': [...], 'dst': [...]} route
// objects as the scheduler log, so each becomes a Record with its Key set. The
// client address is kept in Fields["client_ip"] / Fields["client_port"].
type magclientsrvParser struct{}

func (magclientsrvParser) Name() string { return "magclientsrv" }
func (magclientsrvParser) Kind() Kind   { return KindMagclientsrv }

func (p magclientsrvParser) Parse(raw RawLog) ([]Record, error) {
	if !strings.Contains(raw.Line, "Received Dispatch Request.") || !magclientsrvGate.MatchString(raw.Line) {
		return nil, ErrNoMatch
	}

	// Shared by every record from this line. The client is enrichment only:
	// missing or unsplittable, the line still correlates.
	base := map[string]any{}
	if m := magclientsrvClientRe.FindStringSubmatch(raw.Line); m != nil {
		client := strings.TrimSpace(m[1])
		if host, port, err := net.SplitHostPort(client); err == nil {
			base["client_ip"] = host
			if n, err := strconv.Atoi(port); err == nil {
				base["client_port"] = n
			}
		} else {
			base["client_ip"] = client
		}
	}
	if m := magclientsrvMessageRe.FindStringSubmatch(raw.Line); m != nil {
		base["message_id"] = m[1]
	}

	var records []Record
	for _, obj := range objectRe.FindAllString(raw.Line, -1) {
		if !strings.Contains(obj, "'src'") {
			continue
		}
		src, err := extractOneUUID(schedulerSrcRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: magclientsrv src: %w", err)
		}
		dst, err := extractOneUUID(schedulerDstRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: magclientsrv dst: %w", err)
		}
		fields := make(map[string]any, len(base))
		for k, v := range base {
			fields[k] = v
		}
		records = append(records, Record{
			Kind:   KindMagclientsrv,
			Time:   raw.Time,
			Key:    &Key{Src: src, Dst: dst},
			Fields: fields,
			Raw:    raw.Line,
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("correlator: magclientsrv route line matched but no usable route objects found")
	}
	return records, nil
}
