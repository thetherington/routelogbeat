package correlator

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	magnumAGate = regexp.MustCompile(`Subscribe request\.`)
	// Subscribe-request shape 1 (the original): Dst [('<uuid>',)], Sub [('<uuid>',)]
	magnumADstRe = regexp.MustCompile(`Dst\s*\[([^\]]*)\]`)
	magnumASubRe = regexp.MustCompile(`Sub\s*\[([^\]]*)\]`)

	magnumBGate   = regexp.MustCompile(`Subscription Request Complete:`)
	magnumBRoutes = regexp.MustCompile(`Routes\s*\[([^\]]*)\]`)

	// The {'dst': [...], 'sub_dst': [...]} route object, shared by the
	// "Subscription Request Complete:" line and by subscribe-request shape 2.
	magnumObjDstRe    = regexp.MustCompile(`'dst'\s*:\s*\[([^\]]*)\]`)
	magnumObjSubDstRe = regexp.MustCompile(`'sub_dst'\s*:\s*\[([^\]]*)\]`)
)

// magnumParser recognizes the magnum log formats: "Subscribe request."
// (variant A, KindMagnumSubscribe) and "Subscription Request Complete:"
// (variant B, KindMagnumComplete). Neither opens an envelope by default
// unless configured to via open_on, but either can via the partial-envelope
// fallback.
//
// "Subscribe request." has been seen in two shapes, both accepted and both
// yielding the same Key ({Src: sub, Dst: dst}):
//
//	shape 1  ... Subscribe request. Dst [('<uuid>',)], Sub [('<uuid>',)], User [None], ID [None]
//	shape 2  ... Subscribe request. Subscription [{'dst': ['<uuid>'], 'sub_dst': ['<uuid>']}], User [admin], ID [<uuid>]
//
// Fields["subscribe_shape"] records which one matched ("dst_sub" or
// "subscription"). The trailing ID [...] UUID in shape 2 is a request ID, not
// part of the route, and is ignored.
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
	// Shape 1 has a literal "Dst [" field; shape 2 only has the lower-case
	// 'dst' key inside a Subscription [...] object, so this cleanly tells
	// them apart.
	if magnumADstRe.MatchString(raw.Line) {
		return p.parseADstSub(raw)
	}
	return p.parseASubscription(raw)
}

// parseADstSub handles shape 1: Dst [('<uuid>',)], Sub [('<uuid>',)].
func (p magnumParser) parseADstSub(raw RawLog) ([]Record, error) {
	dst, err := extractOneUUID(magnumADstRe, raw.Line)
	if err != nil {
		return nil, fmt.Errorf("correlator: magnum subscribe Dst: %w", err)
	}
	sub, err := extractOneUUID(magnumASubRe, raw.Line)
	if err != nil {
		return nil, fmt.Errorf("correlator: magnum subscribe Sub: %w", err)
	}
	return []Record{{
		Kind: KindMagnumSubscribe,
		Time: raw.Time,
		Key:  &Key{Src: sub, Dst: dst},
		Fields: map[string]any{
			"magnum_variant":  "subscribe_request",
			"subscribe_shape": "dst_sub",
		},
		Raw: raw.Line,
	}}, nil
}

// parseASubscription handles shape 2: Subscription [{'dst': [...], 'sub_dst': [...]}, ...].
func (p magnumParser) parseASubscription(raw RawLog) ([]Record, error) {
	return magnumObjectRecords(raw, KindMagnumSubscribe, "magnum subscribe subscription", func() map[string]any {
		return map[string]any{
			"magnum_variant":  "subscribe_request",
			"subscribe_shape": "subscription",
		}
	})
}

func (p magnumParser) parseB(raw RawLog) ([]Record, error) {
	if !magnumBGate.MatchString(raw.Line) {
		return nil, ErrNoMatch
	}

	var routes string
	if m := magnumBRoutes.FindStringSubmatch(raw.Line); m != nil {
		routes = strings.TrimSpace(m[1])
	}

	return magnumObjectRecords(raw, KindMagnumComplete, "magnum complete", func() map[string]any {
		return map[string]any{
			"magnum_variant": "request_complete",
			"routes":         routes,
		}
	})
}

// magnumObjectRecords builds one Record per {'dst': [...], 'sub_dst': [...]}
// object in raw.Line, each keyed {Src: sub_dst, Dst: dst}. Every object must
// hold exactly one UUID in each list (these lists never hold more than one
// entry); anything else is a parse error. fields returns a fresh map per
// record, so records never share one. label prefixes errors.
func magnumObjectRecords(raw RawLog, kind Kind, label string, fields func() map[string]any) ([]Record, error) {
	objects := objectRe.FindAllString(raw.Line, -1)
	records := make([]Record, 0, len(objects))
	for _, obj := range objects {
		if !strings.Contains(obj, "'dst'") {
			continue
		}
		dst, err := extractOneUUID(magnumObjDstRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: %s dst: %w", label, err)
		}
		sub, err := extractOneUUID(magnumObjSubDstRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: %s sub_dst: %w", label, err)
		}
		records = append(records, Record{
			Kind:   kind,
			Time:   raw.Time,
			Key:    &Key{Src: sub, Dst: dst},
			Fields: fields(),
			Raw:    raw.Line,
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("correlator: %s line matched but no usable route objects found", label)
	}
	return records, nil
}
