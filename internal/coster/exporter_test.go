package coster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-test/deep"
)

var testBufferingExporterCases = []struct {
	name           string
	datum          []CostData
	expectedBuffer map[CostDataKey]CostData
}{
	{
		name: "Merges related data",
		datum: []CostData{
			CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Value:    5,
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "bar",
				},
				EndTime: time.Unix(1542000000, 0),
			},
			CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Value:    3,
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "bar",
				},
				EndTime: time.Unix(1542000005, 0),
			},
		},
		expectedBuffer: map[CostDataKey]CostData{
			CostDataKey{
				Kind:       ResourceCostWeighted,
				Strategy:   "weighted",
				Dimensions: "component:bar,service:foo",
			}: CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "bar",
				},
				Value:   8,                        // The combined sum.
				EndTime: time.Unix(1542000005, 0), // The last exported timestamp.
			},
		},
	},
	{
		name: "Leaves distinct data separate",
		datum: []CostData{
			CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Value:    5,
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "bar",
				},
				EndTime: time.Unix(1542000000, 0),
			},
			CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Value:    3,
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "baz",
				},
				EndTime: time.Unix(1542000000, 0),
			},
		},
		expectedBuffer: map[CostDataKey]CostData{
			CostDataKey{
				Kind:       ResourceCostWeighted,
				Strategy:   "weighted",
				Dimensions: "component:bar,service:foo",
			}: CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "bar",
				},
				Value:   5,
				EndTime: time.Unix(1542000000, 0),
			},
			CostDataKey{
				Kind:       ResourceCostWeighted,
				Strategy:   "weighted",
				Dimensions: "component:baz,service:foo",
			}: CostData{
				Kind:     ResourceCostWeighted,
				Strategy: "weighted",
				Dimensions: map[string]string{
					"service":   "foo",
					"component": "baz",
				},
				Value:   3,
				EndTime: time.Unix(1542000000, 0),
			},
		},
	},
}

func TestBufferingExporter(t *testing.T) {
	for _, tt := range testBufferingExporterCases {
		t.Run(tt.name, func(t *testing.T) {
			ce := &BufferingCostExporter{
				ctx:      context.Background(),
				buffer:   map[CostDataKey]CostData{},
				interval: time.Second, // Irrelevant in tests.
				mux:      sync.Mutex{},
			}

			for _, cd := range tt.datum {
				ce.ExportCost(cd)
			}

			if diff := deep.Equal(ce.buffer, tt.expectedBuffer); diff != nil {
				t.Fatal(diff)
			}
		})
	}
}
