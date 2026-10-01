package correlator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// knownDsts returns a MetadataResolver that only knows the given destination
// UUIDs (and every source), like a metadata cache scoped to some tags.
func knownDsts(dsts ...string) MetadataResolver {
	known := map[string]bool{}
	for _, d := range dsts {
		known[d] = true
	}
	return MetadataResolverFunc(func(uuid string, role Role) (map[string]any, bool) {
		switch role {
		case RoleDst:
			if !known[uuid] {
				return nil, false
			}
			return map[string]any{"name": "dst-" + uuid[:8]}, true
		case RoleSrc:
			return map[string]any{"name": "src-" + uuid[:8]}, true
		}
		return nil, false
	})
}

func requireDstMetadata(c *Config) { c.RequireDstMetadata = true }

func TestRequireDstMetadata_OffByDefault(t *testing.T) {
	// Default behavior is unchanged: an unknown destination still correlates,
	// just without DstMeta.
	eng, clock := newTestEngine(t, nil, WithMetadataResolver(knownDsts()))

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Nil(t, env.Resolved.DstMeta)
	require.EqualValues(t, 0, eng.Stats().FilteredNoDstMetadata)
}

func TestRequireDstMetadata_KnownDestinationCorrelates(t *testing.T) {
	eng, clock := newTestEngine(t, requireDstMetadata, WithMetadataResolver(knownDsts(testDst)))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: magnumALine(testSrc, testDst), Time: testTime.Add(time.Second)},
	)
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Equal(t, Key{Src: testSrc, Dst: testDst}, env.Key)
	require.Len(t, env.Records, 2)
	require.Equal(t, "dst-d113cd1e", env.Resolved.DstMeta["name"])
	require.Equal(t, "src-3fd8c558", env.Resolved.SrcMeta["name"])
	require.EqualValues(t, 0, eng.Stats().FilteredNoDstMetadata)
}

func TestRequireDstMetadata_UnknownDestinationDiscarded(t *testing.T) {
	eng, clock := newTestEngine(t, requireDstMetadata, WithMetadataResolver(knownDsts( /* none */ )))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		RawLog{Line: magnumALine(testSrc, testDst), Time: testTime.Add(time.Second)},
		RawLog{Line: magnumBLine(testSrc, testDst), Time: testTime.Add(2 * time.Second)},
	)

	s := eng.Stats()
	require.EqualValues(t, 0, s.EnvelopesOpened)
	require.EqualValues(t, 3, s.FilteredNoDstMetadata) // each record tried to open, each was filtered
	require.EqualValues(t, 0, s.ParseErrors)

	clock.Advance(time.Minute)
	expectNoEnvelope(t, eng, 50*time.Millisecond)
}

func TestRequireDstMetadata_OnlyDestinationGates(t *testing.T) {
	// The source has no metadata, the destination does: still correlates.
	meta := MetadataResolverFunc(func(uuid string, role Role) (map[string]any, bool) {
		if role == RoleDst && uuid == testDst {
			return map[string]any{"name": "known"}, true
		}
		return nil, false
	})
	eng, clock := newTestEngine(t, requireDstMetadata, WithMetadataResolver(meta))

	submitAll(t, eng, RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime})
	clock.Advance(time.Minute)

	env := recvEnvelope(t, eng)
	require.Nil(t, env.Resolved.SrcMeta)
	require.Equal(t, "known", env.Resolved.DstMeta["name"])
}

func TestRequireDstMetadata_FilteredRouteDoesNotEvictOrSupersede(t *testing.T) {
	const unknownDst = "77777777-7777-7777-7777-777777777777"
	eng, clock := newTestEngine(t, func(c *Config) {
		c.RequireDstMetadata = true
		c.MaxOpen = 1
	}, WithMetadataResolver(knownDsts(testDst)))

	submitAll(t, eng,
		RawLog{Line: schedulerLine(testSrc, testDst), Time: testTime},
		// At MaxOpen=1 this would evict the envelope above if the filter ran
		// after the eviction step.
		RawLog{Line: schedulerLine(testSrc, unknownDst), Time: testTime.Add(time.Second)},
	)

	s := eng.Stats()
	require.EqualValues(t, 1, s.EnvelopesOpened)
	require.EqualValues(t, 0, s.EnvelopesClosed)
	require.EqualValues(t, 0, s.MaxOpenEvictions)
	require.EqualValues(t, 1, s.FilteredNoDstMetadata)

	clock.Advance(time.Minute)
	env := recvEnvelope(t, eng)
	require.Equal(t, testDst, env.Key.Dst)
	require.Equal(t, ReasonCloseAfter, env.Reason) // closed on schedule, not evicted
}

func TestRequireDstMetadata_NeedsMetadataResolver(t *testing.T) {
	_, err := New(Config{RequireDstMetadata: true})
	require.ErrorContains(t, err, "MetadataResolver")
}
