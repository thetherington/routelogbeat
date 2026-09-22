package correlator

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	magnumAGate  = regexp.MustCompile(`Subscribe request\.`)
	magnumADstRe = regexp.MustCompile(`Dst\s*\[([^\]]*)\]`)
	magnumASubRe = regexp.MustCompile(`Sub\s*\[([^\]]*)\]`)

	magnumBGate   = regexp.MustCompile(`Subscription Request Complete:`)
	magnumBDstRe  = regexp.MustCompile(`'dst'\s*:\s*\[([^\]]*)\]`)
	magnumBSubRe  = regexp.MustCompile(`'sub_dst'\s*:\s*\[([^\]]*)\]`)
	magnumBRoutes = regexp.MustCompile(`Routes\s*\[([^\]]*)\]`)
)

// magnumParser recognizes the two magnum log formats: "Subscribe request."
// (variant A, KindMagnumSubscribe) and "Subscription Request Complete:"
// (variant B, KindMagnumComplete). Neither opens an envelope by default
// unless configured to via open_on, but either can via the partial-envelope
// fallback.
type magnumParser struct{}

func (magnumParser) Name() string { return "magnum" }
func (magnumParser) Kind() Kind   { return KindMagnumSubscribe }

func (p magnumParser) Parse(raw RawLog) ([]Record, error) {
	switch {
	case strings.Contains(raw.Line, "Subscribe request."):
		return p.parseA(raw)
	case strings.Contains(raw.Line, "Subscription Request Complete:"):
		return p.parseB(raw)
	default:
		return nil, ErrNoMatch
	}
}

func (p magnumParser) parseA(raw RawLog) ([]Record, error) {
	if !magnumAGate.MatchString(raw.Line) {
		return nil, ErrNoMatch
	}
	dst, err := extractOneUUID(magnumADstRe, raw.Line)
	if err != nil {
		return nil, fmt.Errorf("correlator: magnum subscribe Dst: %w", err)
	}
	sub, err := extractOneUUID(magnumASubRe, raw.Line)
	if err != nil {
		return nil, fmt.Errorf("correlator: magnum subscribe Sub: %w", err)
	}
	return []Record{{
		Kind:   KindMagnumSubscribe,
		Time:   raw.Time,
		Key:    &Key{Src: sub, Dst: dst},
		Fields: map[string]any{"magnum_variant": "subscribe_request"},
		Raw:    raw.Line,
	}}, nil
}

func (p magnumParser) parseB(raw RawLog) ([]Record, error) {
	if !magnumBGate.MatchString(raw.Line) {
		return nil, ErrNoMatch
	}

	var routes string
	if m := magnumBRoutes.FindStringSubmatch(raw.Line); m != nil {
		routes = strings.TrimSpace(m[1])
	}

	objects := objectRe.FindAllString(raw.Line, -1)
	records := make([]Record, 0, len(objects))
	for _, obj := range objects {
		if !strings.Contains(obj, "'dst'") {
			continue
		}
		dst, err := extractOneUUID(magnumBDstRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: magnum complete dst: %w", err)
		}
		sub, err := extractOneUUID(magnumBSubRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: magnum complete sub_dst: %w", err)
		}
		records = append(records, Record{
			Kind: KindMagnumComplete,
			Time: raw.Time,
			Key:  &Key{Src: sub, Dst: dst},
			Fields: map[string]any{
				"magnum_variant": "request_complete",
				"routes":         routes,
			},
			Raw: raw.Line,
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("correlator: magnum complete line matched but no usable route objects found")
	}
	return records, nil
}
