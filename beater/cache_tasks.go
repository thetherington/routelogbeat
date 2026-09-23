package beater

import (
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/thetherington/routelogbeat/beater/correlator"
	"github.com/thetherington/routelogbeat/beater/magnumclient"
)

func (bt *routelogbeat) ProcessSlabTerminal(edges []magnumclient.Edge) error {
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
