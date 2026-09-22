package beater

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestToRawLog(t *testing.T) {
	t.Run("maps Line, Time, and Hostname from the right fields", func(t *testing.T) {
		deviceTime := time.Date(2026, 8, 23, 4, 0, 12, 641_000_000, time.UTC)
		atTime := time.Date(2026, 8, 23, 4, 0, 13, 0, time.UTC)
		msg := &SyslogMessage{
			Timestamp: atTime,
			Device:    Device{Timestamp: deviceTime},
			Log:       Log{Syslog: Syslog{Message: `AuditSet:DST 4 ADDR:"239.32.111.55"`}},
			Annotation: Annotation{General: General{
				DeviceName: "sv7bc-slab027",
				DeviceType: "slab",
			}},
		}

		raw, ok := toRawLog(msg)
		require.True(t, ok)
		require.Equal(t, `AuditSet:DST 4 ADDR:"239.32.111.55"`, raw.Line)
		require.Equal(t, deviceTime, raw.Time)
		require.Equal(t, "sv7bc-slab027", raw.Hostname)
	})

	t.Run("zero Device.Timestamp falls back to @timestamp", func(t *testing.T) {
		atTime := time.Date(2026, 8, 23, 4, 0, 13, 0, time.UTC)
		msg := &SyslogMessage{
			Timestamp: atTime,
			Log:       Log{Syslog: Syslog{Message: "some log line"}},
		}

		raw, ok := toRawLog(msg)
		require.True(t, ok)
		require.Equal(t, atTime, raw.Time)
	})

	t.Run("both timestamps zero falls back to time.Now", func(t *testing.T) {
		msg := &SyslogMessage{Log: Log{Syslog: Syslog{Message: "some log line"}}}

		before := time.Now()
		raw, ok := toRawLog(msg)
		after := time.Now()

		require.True(t, ok)
		require.False(t, raw.Time.Before(before))
		require.False(t, raw.Time.After(after))
	})

	t.Run("empty Log.Syslog.Message is not ok", func(t *testing.T) {
		msg := &SyslogMessage{}
		_, ok := toRawLog(msg)
		require.False(t, ok)
	})
}
