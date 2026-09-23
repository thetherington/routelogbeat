package beater

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Extracts output from magnum port terminal by the last number from a string like "[111,8,2,32]".
func ExtractOutputFromPort(s string) (int, error) {
	trimmed := strings.TrimFunc(s, func(r rune) bool {
		return !unicode.IsDigit(r) && r != ',' && r != '-'
	})
	parts := strings.Split(trimmed, ",")
	if len(parts) == 0 {
		return 0, fmt.Errorf("no numbers found")
	}
	last := strings.TrimSpace(parts[len(parts)-1])
	return strconv.Atoi(last)
}
