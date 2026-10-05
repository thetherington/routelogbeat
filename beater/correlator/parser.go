package correlator

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrNoMatch signals that a line is not this parser's format; the engine
// tries the next parser in the configured set.
var ErrNoMatch = errors.New("correlator: line did not match this parser")

// Parser recognizes one log format and turns a matching line into Records.
type Parser interface {
	// Name is the coarse label stamped onto every Record this parser
	// returns: "scheduler" | "magnum" | "slab" | "magrtrsrv" | "magclientsrv".
	Name() string
	// Kind is this parser's dominant Kind. magnum refines it per-variant
	// inside Parse.
	Kind() Kind
	// Parse returns ErrNoMatch if raw.Line is not this parser's format (the
	// engine tries the next parser); a different, non-nil error if the line
	// matched the parser's shape but its body could not be extracted;
	// otherwise one Record per matched route/observation.
	Parse(raw RawLog) ([]Record, error)
}

// DefaultParsers returns the built-in parsers in the order ingest() tries
// them. The five formats are disjoint, so order is not correctness-critical,
// only a minor perf choice (put the hottest formats first).
func DefaultParsers() []Parser {
	return []Parser{
		schedulerParser{},
		magclientsrvParser{},
		magnumParser{},
		slabParser{},
		magrtrsrvParser{},
	}
}

// uuidRe matches a UUID in either case; extractOneUUID lower-cases the result.
var uuidRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// objectRe matches one innermost {...} object (no nested braces) — used to
// walk a Python-ish list-of-dicts literal without a full parser. An object
// with nested braces (e.g. the dcpipes wrapper {'params': ..., 'method': ...})
// never matches this.
var objectRe = regexp.MustCompile(`\{[^{}]*\}`)

// extractOneUUID runs re against s, expecting capture group 1 to be a
// Python-list literal containing UUIDs, and requires it to contain exactly
// one — these lists never hold more than one entry (confirmed with the
// user), so zero or several is a parse error rather than something to pair
// or cross-multiply.
func extractOneUUID(re *regexp.Regexp, s string) (string, error) {
	m := re.FindStringSubmatch(s)
	if m == nil {
		return "", fmt.Errorf("field not found")
	}
	uuids := uuidRe.FindAllString(m[1], -1)
	if len(uuids) != 1 {
		return "", fmt.Errorf("expected exactly one UUID, found %d", len(uuids))
	}
	return strings.ToLower(uuids[0]), nil
}
