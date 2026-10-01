package beater

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/thetherington/routelogbeat/beater/correlator"
	"github.com/thetherington/routelogbeat/beater/magnumclient"
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

// toRawLog maps a decoded SyslogMessage to the correlator engine's
// normalized input. There is no type tag on the message — Line is handed to
// the engine as-is and its parsers identify (or discard) it by content.
func toRawLog(msg *SyslogMessage) (correlator.RawLog, bool) {
	line := msg.Log.Syslog.Message
	if line == "" {
		return correlator.RawLog{}, false
	}

	t := msg.Device.Timestamp
	if t.IsZero() {
		t = msg.Timestamp
	}
	if t.IsZero() {
		t = time.Now()
	}

	return correlator.RawLog{
		Line:     line,
		Time:     t,
		Hostname: msg.Annotation.General.DeviceName,
	}, true
}

// findNamesetValueByName searches for a nameset value by its name within a slice of NamesetName.
// If the name is found, it returns the corresponding value; otherwise, it returns the default value.
func findNamesetValueByName(s string, namesetName []magnumclient.NamesetName, defaultValue string) string {
	for _, n := range namesetName {
		if n.Nameset.Name == s {
			return n.Name
		}
	}

	return defaultValue
}
