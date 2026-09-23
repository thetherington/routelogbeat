package beater

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thetherington/routelogbeat/beater/correlator"
)

func TestUnresolvedHint(t *testing.T) {
	tests := []struct {
		name string
		ev   correlator.UnresolvedEvent
		want string // substring the hint must contain
	}{
		{
			name: "no envelope open",
			ev:   correlator.UnresolvedEvent{Reason: correlator.MissNoSlabMatch, OpenEnvelopes: 0},
			want: "no envelope is open",
		},
		{
			name: "hostname known under a different DST number",
			ev: correlator.UnresolvedEvent{
				Reason: correlator.MissNoSlabMatch, OpenEnvelopes: 2, DstNum: 4, Hostname: "sv7bc-slab058",
				KnownDstNumsForHostname: []int{3},
			},
			want: "disagree on the DST number",
		},
		{
			name: "DST number known under a different hostname",
			ev: correlator.UnresolvedEvent{
				Reason: correlator.MissNoSlabMatch, OpenEnvelopes: 2, DstNum: 4, Hostname: "sv7bc-slab058",
				KnownHostnamesForDstNum: []string{"sv7bc-slab027"},
			},
			want: "disagree on the hostname",
		},
		{
			name: "open envelopes without a slab list",
			ev:   correlator.UnresolvedEvent{Reason: correlator.MissNoSlabMatch, OpenEnvelopes: 3, OpenWithoutSlabs: 2},
			want: "resolver returned nothing",
		},
		{
			name: "slab belongs to some other destination",
			ev:   correlator.UnresolvedEvent{Reason: correlator.MissNoSlabMatch, OpenEnvelopes: 1},
			want: "different destination",
		},
		{
			name: "multicast conflict",
			ev: correlator.UnresolvedEvent{
				Reason: correlator.MissMulticastConflict, Multicast: "239.32.111.99", ExpectedMulticast: "239.32.111.55",
			},
			want: "239.32.111.55",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Contains(t, unresolvedHint(tt.ev), tt.want)
		})
	}
}
