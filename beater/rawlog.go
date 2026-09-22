package beater

import (
	"time"

	"github.com/thetherington/routelogbeat/beater/correlator"
)

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
