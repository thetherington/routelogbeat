package beater

import (
	"slices"

	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/thetherington/routelogbeat/beater/correlator"
	"github.com/thetherington/routelogbeat/beater/magnumclient"
)

func (bt *routelogbeat) ProcessSlabTerminal(edges []magnumclient.Edge, opts ...magnumclient.CallbackOptions) error {
	logp.Debug("ProcessSlabTerminal", "Scanning %d events to update SlabMap Cache", len(edges))

	for _, edge := range edges {
		var (
			deviceName string
			output     int
			subId      string
		)

		// Extract the subscribed source ID if available. This will be used as the key in the bus routing cache.
		if edge.SubscribedSource != nil {
			subId = edge.SubscribedSource.Id
		}

		// Skip processing if there is no subscribed source ID.
		if subId == "" {
			continue
		}

		// Extract the device name and output from the port if available.
		if edge.Port != nil {
			deviceName = edge.Port.Device.Name

			outputInt, err := ExtractOutputFromPort(edge.Port.Id)
			if err != nil {
				logp.Err("failed to extract output from port Id: %s, error: %v", edge.Port.Id, err)
				continue
			}

			output = outputInt
		}

		p := Slab{
			Id:     edge.Id,
			Name:   edge.Name,
			Device: deviceName,
			Output: output,
		}

		if SlabMap.Has(subId) {
			// If there is already a slab or slabs for this subscribed source, check if the current slab is already present.
			SlabMap.DoMutSet(subId, func(slabs Slabs) Slabs {
				found := false
				for i, slab := range slabs {
					if slab.Id == p.Id {
						slabs[i] = p
						found = true
						break
					}
				}
				if !found {
					slabs = append(slabs, p)
				}
				return slabs
			})
		} else {
			SlabMap.Set(subId, []Slab{p})
		}
	}

	return nil
	// logp.Debug("ScanSlabTerminal", "ScanEvent for: %s (%s) Output: %d SubscribedSource: %s", edge.Name, deviceName, output, subId)
}

// SlabMapResolver resolves the slabs for a given destination UUID from the SlabMap cache. It returns a slice of correlator.SlabRef and a boolean indicating if the slabs were found.
func SlabMapResolver(dstUUID string) ([]correlator.SlabRef, bool) {
	slabs, ok := SlabMap.Get(dstUUID)
	if !ok {
		return nil, false
	}

	var slabRefs []correlator.SlabRef
	for _, slab := range slabs {
		slabRefs = append(slabRefs, correlator.SlabRef{
			Hostname: slab.Device,
			DstNum:   slab.Output,
		})
	}

	return slabRefs, true
}

func (bt *routelogbeat) ProcessSubTerminal(edges []magnumclient.Edge, opts ...magnumclient.CallbackOptions) error {
	logp.Debug("ProcessSubTerminal", "Scanning %d events to update DestinationMap/SourceMap Cache", len(edges))

	var (
		tag           string
		isDestination bool
		isSource      bool
	)

	if len(opts) > 0 {
		if t, ok := opts[0]["tag"]; ok {
			tag = t
		}
	}

	// check if tag is in the configuration for either destinations or sources
	if slices.Contains(bt.config.Destinations, tag) {
		isDestination = true
	} else if slices.Contains(bt.config.Sources, tag) {
		isSource = true
	}

	for _, edge := range edges {
		var terminal Terminal

		terminal.Tag = tag
		terminal.Id = edge.Id
		terminal.Name = edge.Name
		terminal.Label = findNamesetValueByName(bt.config.Mapping.Nameset, edge.NamesetNames, bt.config.Mapping.Default)

		if isDestination {
			DestinationMap.Set(terminal.Id, terminal)
		} else if isSource {
			SourceMap.Set(terminal.Id, terminal)
		}
	}

	return nil
}

func TerminalMapResolver(uuid string, role correlator.Role) (map[string]any, bool) {
	var terminal Terminal

	switch role {
	case correlator.RoleDst:
		t, ok := DestinationMap.Get(uuid)
		if !ok {
			return nil, false
		}
		terminal = t

	case correlator.RoleSrc:
		t, ok := SourceMap.Get(uuid)
		if !ok {
			return nil, false
		}
		terminal = t

	default:
		return nil, false
	}

	return map[string]any{
		"tag":   terminal.Tag,
		"id":    terminal.Id,
		"name":  terminal.Name,
		"label": terminal.Label,
	}, true
}
