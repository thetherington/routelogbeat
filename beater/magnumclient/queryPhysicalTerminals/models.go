package queryphysicalterminals

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type QueryTerminals struct {
	Terminals Terminals `graphql:"terminals(input: {filters: [{id: \"isTlr\", booleanValue: true}, {id: \"isDst\", booleanValue: true}, {id:\"isSub\", booleanValue: false}, {id: \"tags\", listIncludes: [$tag]}]})"`
}

type Terminals struct {
	TotalCount int
	Edges      []Edge `graphql:"edges(limit: $limit, offset: $offset)"`
}

type Edge struct {
	Id                        string
	Name                      string
	Port                      *Port
	RouteableTerminalFragment `graphql:"... on RouteableTerminal"`
}

type Port struct {
	Id     string
	Name   string
	Device Device
}

type Device struct {
	Name string
}

type RouteableTerminalFragment struct {
	SubscribedSource *SubscribedSource
}

type SubscribedSource struct {
	Id   string
	Name string
}

// Extracts output from magnum port terminal by the last number from a string like "[111,8,2,32]".
func (p Port) ExtractOutputFromPort() (int, error) {
	trimmed := strings.TrimFunc(p.Id, func(r rune) bool {
		return !unicode.IsDigit(r) && r != ',' && r != '-'
	})
	parts := strings.Split(trimmed, ",")
	if len(parts) == 0 {
		return 0, fmt.Errorf("no numbers found")
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	return strconv.Atoi(last)
}
