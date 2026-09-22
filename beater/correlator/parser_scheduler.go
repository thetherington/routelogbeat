package correlator

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	schedulerV1Gate = regexp.MustCompile(`Sending route:\s*(\[\[.*\]\])`)
	schedulerV2Gate = regexp.MustCompile(`jsonrpctcp:\s*SENDING:.*'method':\s*'route'`)
	schedulerSrcRe  = regexp.MustCompile(`'src'\s*:\s*\[([^\]]*)\]`)
	schedulerDstRe  = regexp.MustCompile(`'dst'\s*:\s*\[([^\]]*)\]`)
)

// schedulerParser recognizes two scheduler log formats — the "main: Sending
// route: ..." format (V1) and the "dcpipes.jsonrpctcp: SENDING: ..." format
// (V2) — both carrying the same {'src': [...], 'dst': [...]} route objects,
// only the wrapper differs. Both open an envelope; both are KindScheduler,
// distinguished only by Fields["scheduler_variant"].
type schedulerParser struct{}

func (schedulerParser) Name() string { return "scheduler" }
func (schedulerParser) Kind() Kind   { return KindScheduler }

func (p schedulerParser) Parse(raw RawLog) ([]Record, error) {
	hasV1 := strings.Contains(raw.Line, "Sending route:")
	hasV2 := strings.Contains(raw.Line, "jsonrpctcp") && strings.Contains(raw.Line, "'method': 'route'")
	if !hasV1 && !hasV2 {
		return nil, ErrNoMatch
	}

	var variant string
	switch {
	case hasV1 && schedulerV1Gate.MatchString(raw.Line):
		variant = "sending_route"
	case hasV2 && schedulerV2Gate.MatchString(raw.Line):
		variant = "dcpipes"
	default:
		return nil, ErrNoMatch
	}

	objects := objectRe.FindAllString(raw.Line, -1)
	records := make([]Record, 0, len(objects))
	for _, obj := range objects {
		if !strings.Contains(obj, "'src'") {
			// The dcpipes outer {'params': ..., 'method': 'route'} object has
			// nested braces so objectRe never matches it in practice; this
			// guard is defensive in case some other {...} object slips in.
			continue
		}
		src, err := extractOneUUID(schedulerSrcRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: scheduler src: %w", err)
		}
		dst, err := extractOneUUID(schedulerDstRe, obj)
		if err != nil {
			return nil, fmt.Errorf("correlator: scheduler dst: %w", err)
		}
		records = append(records, Record{
			Kind:   KindScheduler,
			Time:   raw.Time,
			Key:    &Key{Src: src, Dst: dst},
			Fields: map[string]any{"scheduler_variant": variant},
			Raw:    raw.Line,
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("correlator: scheduler line matched but no usable route objects found")
	}
	return records, nil
}
