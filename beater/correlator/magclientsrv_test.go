package correlator

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Tests for the magclientsrv log: parsing, and how it opens and enriches
// envelopes.

func magclientsrvLine(src, dst, method string) string {
	return `INFO:interfaces.server:Received Dispatch Request. Client [10.103.40.46:45662], Message ID [44208], Method [` +
		method + `], Parameters [[[{'src': ['` + src + `'], 'dst': ['` + dst + `']}]]].`
}

func TestMagclientsrvParser(t *testing.T) {
	p := magclientsrvParser{}
	require.Equal(t, "magclientsrv", p.Name())
	require.Equal(t, KindMagclientsrv, p.Kind())

	t.Run("real sample line", func(t *testing.T) {
		line := `INFO:interfaces.server:Received Dispatch Request. Client [10.103.40.46:45662], Message ID [44208], Method [route], Parameters [[[{'src': ['def463b1-8087-581e-8a87-df52798bb35c'], 'dst': ['46b26e14-b2c7-5d24-a0fa-3b24fa395091']}]]].`
		recs, err := p.Parse(RawLog{Line: line, Time: testTime})
		require.NoError(t, err)
		require.Len(t, recs, 1)
		require.Equal(t, KindMagclientsrv, recs[0].Kind)
		require.Equal(t, &Key{Src: "def463b1-8087-581e-8a87-df52798bb35c", Dst: "46b26e14-b2c7-5d24-a0fa-3b24fa395091"}, recs[0].Key)
		require.Equal(t, "10.103.40.46", recs[0].Fields["client_ip"])
		require.Equal(t, 45662, recs[0].Fields["client_port"])
		require.Equal(t, "44208", recs[0].Fields["message_id"])
		require.Equal(t, testTime, recs[0].Time)
	})

	t.Run("uppercase uuid lower-cased", func(t *testing.T) {
		recs, err := p.Parse(RawLog{Line: magclientsrvLine(strings.ToUpper(testSrc), testDst, "route")})
		require.NoError(t, err)
		require.Equal(t, testSrc, recs[0].Key.Src)
	})

	t.Run("other methods do not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: magclientsrvLine(testSrc, testDst, "subscribe")})
		require.ErrorIs(t, err, ErrNoMatch)
		_, err = p.Parse(RawLog{Line: magclientsrvLine(testSrc, testDst, "router")})
		require.ErrorIs(t, err, ErrNoMatch)
	})

	t.Run("route with no route object is a parse error", func(t *testing.T) {
		line := `INFO:interfaces.server:Received Dispatch Request. Client [10.1.1.1:1], Message ID [1], Method [route], Parameters [[]].`
		_, err := p.Parse(RawLog{Line: line})
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrNoMatch)
	})

	t.Run("multi-entry src list is a parse error", func(t *testing.T) {
		line := strings.Replace(magclientsrvLine(testSrc, testDst, "route"), `'src': ['`+testSrc+`']`,
			`'src': ['`+testSrc+`', '`+testDst+`']`, 1)
		_, err := p.Parse(RawLog{Line: line})
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrNoMatch)
	})

	t.Run("multiple route objects yield one record each, sharing the client", func(t *testing.T) {
		other := "99999999-9999-9999-9999-999999999999"
		line := `INFO:interfaces.server:Received Dispatch Request. Client [10.103.40.46:45662], Message ID [7], Method [route], Parameters [[[{'src': ['` +
			testSrc + `'], 'dst': ['` + testDst + `']}, {'src': ['` + testSrc + `'], 'dst': ['` + other + `']}]]].`
		recs, err := p.Parse(RawLog{Line: line})
		require.NoError(t, err)
		require.Len(t, recs, 2)
		require.Equal(t, other, recs[1].Key.Dst)
		require.Equal(t, "10.103.40.46", recs[1].Fields["client_ip"])
	})

	t.Run("client without a port keeps the whole value as the ip", func(t *testing.T) {
		line := strings.Replace(magclientsrvLine(testSrc, testDst, "route"), "10.103.40.46:45662", "10.103.40.46", 1)
		recs, err := p.Parse(RawLog{Line: line})
		require.NoError(t, err)
		require.Equal(t, "10.103.40.46", recs[0].Fields["client_ip"])
		_, hasPort := recs[0].Fields["client_port"]
		require.False(t, hasPort)
	})

	t.Run("ipv6 client", func(t *testing.T) {
		line := strings.Replace(magclientsrvLine(testSrc, testDst, "route"), "10.103.40.46:45662", "[fd00::1]:5000", 1)
		recs, err := p.Parse(RawLog{Line: line})
		require.NoError(t, err)
		require.Equal(t, "fd00::1", recs[0].Fields["client_ip"])
		require.Equal(t, 5000, recs[0].Fields["client_port"])
	})

	t.Run("unrelated line does not match", func(t *testing.T) {
		_, err := p.Parse(RawLog{Line: "INFO:interfaces.server:Client connected"})
		require.ErrorIs(t, err, ErrNoMatch)
	})

	t.Run("no other parser claims the line", func(t *testing.T) {
		raw := RawLog{Line: magclientsrvLine(testSrc, testDst, "route"), Hostname: "magnum"}
		for _, other := range DefaultParsers() {
			if other.Name() == "magclientsrv" {
				continue
			}
			_, err := other.Parse(raw)
			require.ErrorIs(t, err, ErrNoMatch, other.Name())
		}
	})
}

func TestEngine_MagclientsrvOpensNonPartialEnvelope(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	submitAll(t, eng, RawLog{Line: magclientsrvLine(testSrc, testDst, "route"), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, KindMagclientsrv, env.OpenedBy)
	require.False(t, env.Partial)
	require.Equal(t, "10.103.40.46", env.Resolved.ClientIP)
	require.Equal(t, 45662, env.Resolved.ClientPort)
	require.Equal(t, map[string]int{"magclientsrv": 1}, env.SourceCounts())
}

func TestEngine_MagclientsrvAndSchedulerAtSameTime(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	// Low latency: magclientsrv may even be read before the scheduler line.
	submitAll(t, eng,
		RawLog{Line: magclientsrvLine(testSrc, testDst, "route"), Time: testTime},
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: magnumALine(testSrc, testDst), Time: testTime.Add(2 * time.Second)},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.False(t, env.Partial)
	require.Equal(t, map[string]int{"magclientsrv": 1, "scheduler": 1, "magnum": 1}, env.SourceCounts())
	require.Equal(t, testTime, env.OpenedAt)
	require.Equal(t, 2*time.Second, env.Duration())
}

func TestEngine_MagclientsrvClearsPartial(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	submitAll(t, eng,
		RawLog{Line: magnumALine(testSrc, testDst), Time: testTime}, // opens partial
		RawLog{Line: magclientsrvLine(testSrc, testDst, "route"), Time: testTime.Add(time.Second)},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, KindMagnumSubscribe, env.OpenedBy)
	require.False(t, env.Partial)
}

func TestEngine_FirstMagclientsrvClientWins(t *testing.T) {
	eng, clock := newTestEngine(t, nil)

	second := strings.Replace(magclientsrvLine(testSrc, testDst, "route"), "10.103.40.46:45662", "10.0.0.9:1234", 1)
	submitAll(t, eng,
		RawLog{Line: magclientsrvLine(testSrc, testDst, "route"), Time: testTime},
		RawLog{Line: second, Time: testTime.Add(time.Second)},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, "10.103.40.46", env.Resolved.ClientIP)
	require.Equal(t, 45662, env.Resolved.ClientPort)
	require.Equal(t, 2, env.SourceCounts()["magclientsrv"])
}

func TestEngine_OpenOnMagclientsrv(t *testing.T) {
	kind, ok := ParseKind("magclientsrv")
	require.True(t, ok)
	require.Equal(t, "magclientsrv", kind.String())
	require.Equal(t, "magclientsrv", kind.Family())

	eng, clock := newTestEngine(t, func(c *Config) { c.OpenOn = KindMagclientsrv })
	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	// With open_on: magclientsrv, a scheduler-only route is partial.
	env := recvEnvelope(t, eng)
	require.True(t, env.Partial)
}

func TestEngine_OtherMagclientsrvMethodsDiscarded(t *testing.T) {
	eng, _ := newTestEngine(t, nil)

	submitAll(t, eng, RawLog{Line: magclientsrvLine(testSrc, testDst, "subscribe"), Time: testTime})
	s := eng.Stats()
	require.EqualValues(t, 1, s.Discarded)
	require.EqualValues(t, 0, s.EnvelopesOpened)
}
