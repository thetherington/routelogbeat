package correlator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapResolver(t *testing.T) {
	t.Run("Store then Lookup returns the full slab list", func(t *testing.T) {
		m := NewMapResolver()
		m.Store([]Entry{
			{DstUUID: "d1", Slabs: []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}, {Hostname: "sv7bc-slab058", DstNum: 4}}},
			{DstUUID: "d2", Slabs: []SlabRef{{Hostname: "sv7bc-slab099", DstNum: 7}}},
		})

		slabs, ok := m.Lookup("d1")
		require.True(t, ok)
		require.Equal(t, []SlabRef{{Hostname: "sv7bc-slab027", DstNum: 4}, {Hostname: "sv7bc-slab058", DstNum: 4}}, slabs)

		slabs, ok = m.Lookup("d2")
		require.True(t, ok)
		require.Equal(t, []SlabRef{{Hostname: "sv7bc-slab099", DstNum: 7}}, slabs)
	})

	t.Run("unknown destination misses", func(t *testing.T) {
		m := NewMapResolver()
		m.Store([]Entry{{DstUUID: "d1", Slabs: []SlabRef{{Hostname: "h", DstNum: 1}}}})

		_, ok := m.Lookup("unknown")
		require.False(t, ok)
	})

	t.Run("Upsert replaces just one destination", func(t *testing.T) {
		m := NewMapResolver()
		m.Store([]Entry{
			{DstUUID: "d1", Slabs: []SlabRef{{Hostname: "h1", DstNum: 1}}},
			{DstUUID: "d2", Slabs: []SlabRef{{Hostname: "h2", DstNum: 2}}},
		})

		m.Upsert("d1", []SlabRef{{Hostname: "h1-new", DstNum: 9}})

		slabs, ok := m.Lookup("d1")
		require.True(t, ok)
		require.Equal(t, []SlabRef{{Hostname: "h1-new", DstNum: 9}}, slabs)

		slabs, ok = m.Lookup("d2") // untouched
		require.True(t, ok)
		require.Equal(t, []SlabRef{{Hostname: "h2", DstNum: 2}}, slabs)
	})

	t.Run("Store fully replaces the table", func(t *testing.T) {
		m := NewMapResolver()
		m.Store([]Entry{{DstUUID: "d1", Slabs: []SlabRef{{Hostname: "h1", DstNum: 1}}}})
		m.Store([]Entry{{DstUUID: "d2", Slabs: []SlabRef{{Hostname: "h2", DstNum: 2}}}})

		_, ok := m.Lookup("d1")
		require.False(t, ok, "d1 was present before the second Store but absent from it")

		_, ok = m.Lookup("d2")
		require.True(t, ok)
	})

	t.Run("concurrent Store/Upsert and Lookup do not race", func(t *testing.T) {
		m := NewMapResolver()
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(2)
			go func(i int) {
				defer wg.Done()
				m.Upsert("d", []SlabRef{{Hostname: "h", DstNum: i}})
			}(i)
			go func() {
				defer wg.Done()
				m.Lookup("d")
			}()
		}
		wg.Wait()
	})
}

func TestResolverFunc(t *testing.T) {
	cache := map[string][]SlabRef{"d1": {{Hostname: "h1", DstNum: 1}}}
	var r Resolver = ResolverFunc(func(dstUUID string) ([]SlabRef, bool) {
		slabs, ok := cache[dstUUID]
		return slabs, ok
	})

	slabs, ok := r.Lookup("d1")
	require.True(t, ok)
	require.Equal(t, []SlabRef{{Hostname: "h1", DstNum: 1}}, slabs)

	_, ok = r.Lookup("unknown")
	require.False(t, ok)
}

func TestNopResolverAlwaysMisses(t *testing.T) {
	var r Resolver = nopResolver{}
	slabs, ok := r.Lookup("anything")
	require.False(t, ok)
	require.Nil(t, slabs)
}

func TestMetadataResolverFunc(t *testing.T) {
	srcMeta := map[string]map[string]any{"s1": {"name": "source-one"}}
	dstMeta := map[string]map[string]any{"d1": {"name": "dest-one", "device": "sv7bc"}}

	var receivedRole Role
	var receivedUUID string

	var r MetadataResolver = MetadataResolverFunc(func(uuid string, role Role) (map[string]any, bool) {
		receivedUUID, receivedRole = uuid, role
		switch role {
		case RoleSrc:
			m, ok := srcMeta[uuid]
			return m, ok
		case RoleDst:
			m, ok := dstMeta[uuid]
			return m, ok
		default:
			return nil, false
		}
	})

	m, ok := r.Metadata("s1", RoleSrc)
	require.True(t, ok)
	require.Equal(t, "source-one", m["name"])
	require.Equal(t, "s1", receivedUUID)
	require.Equal(t, RoleSrc, receivedRole)

	m, ok = r.Metadata("d1", RoleDst)
	require.True(t, ok)
	require.Equal(t, "dest-one", m["name"])
	require.Equal(t, RoleDst, receivedRole)

	_, ok = r.Metadata("unknown", RoleSrc)
	require.False(t, ok)
}

func TestRoleString(t *testing.T) {
	require.Equal(t, "src", RoleSrc.String())
	require.Equal(t, "dst", RoleDst.String())
}
