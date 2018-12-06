package coster

import (
	"testing"
	"time"
)

var (
	singleLabelCostTableEntry = CostTableEntry{
		Labels: Labels{"beta.kubernetes.io/instance-type": "n1-standard-16"},
	}
	regionZoneAndInstanceTypeCostTableEntry = CostTableEntry{
		Labels: Labels{
			"beta.kubernetes.io/instance-type":         "n1-standard-16",
			"failure-domain.beta.kubernetes.io/region": "us-central1",
			"failure-domain.beta.kubernetes.io/zone":   "us-central1-b",
		},
	}
	regionAndInstanceTypeCostTableEntry = CostTableEntry{
		Labels: Labels{
			"beta.kubernetes.io/instance-type":         "n1-standard-16",
			"failure-domain.beta.kubernetes.io/region": "us-central1",
		},
	}
	fallbackCostTableEntry = CostTableEntry{
		Labels: Labels{},
	}
)

var costTableCases = []struct {
	name        string
	expectedErr error
	table       CostTable

	labels        Labels
	expectedEntry *CostTableEntry
}{
	{
		name:        "null case",
		expectedErr: ErrNoCostEntry,
		table: CostTable{
			Entries: []*CostTableEntry{},
		},
	},
	{
		name: "first match wins",
		table: CostTable{
			Entries: []*CostTableEntry{
				&singleLabelCostTableEntry,
			},
		},
		labels:        singleLabelCostTableEntry.Labels,
		expectedEntry: &singleLabelCostTableEntry,
	},
	{
		name: "empty special case",
		table: CostTable{
			Entries: []*CostTableEntry{
				&fallbackCostTableEntry,
			},
		},
		expectedEntry: &fallbackCostTableEntry,
	},
	{
		name: "cost table entries match if all entry labels are matched",
		table: CostTable{
			Entries: []*CostTableEntry{
				&regionAndInstanceTypeCostTableEntry,
			},
		},
		labels: Labels{
			"beta.kubernetes.io/instance-type":         "n1-standard-16",
			"failure-domain.beta.kubernetes.io/region": "us-central1",
			"another-ignored-label":                    "I wont affect a match",
		},
		expectedEntry: &regionAndInstanceTypeCostTableEntry,
	},
	{
		name: "cost table entries match if all entry labels are matched and honor ordering",
		table: CostTable{
			Entries: []*CostTableEntry{
				&regionAndInstanceTypeCostTableEntry,
				&regionZoneAndInstanceTypeCostTableEntry,
			},
		},
		labels: Labels{
			"beta.kubernetes.io/instance-type":         "n1-standard-16",
			"failure-domain.beta.kubernetes.io/region": "us-central1",
			"failure-domain.beta.kubernetes.io/zone":   "us-central1-b",
		},
		// This may seem uninuitive, but we've listed the regionAndInstanceType entry before the
		// arguably, more precise regionZoneAndInstanceType entry.
		expectedEntry: &regionAndInstanceTypeCostTableEntry,
	},
}

func TestFindByLabels(t *testing.T) {
	for _, tt := range costTableCases {
		t.Run(tt.name, func(t *testing.T) {
			ct := tt.table
			ce, err := ct.FindByLabels(tt.labels)
			if tt.expectedErr != err {
				t.Fatalf("expected error %#v, got %#v", tt.expectedErr, err)
			}
			if tt.expectedEntry != ce {
				t.Fatalf("expected entry %#v, got %#v", tt.expectedEntry, ce)
			}
		})
	}
}

var (
	singleCPU32MebEntry = &CostTableEntry{
		HourlyMemoryByteCostMicroCents: 1,
		HourlyMilliCPUCostMicroCents:   15000,
	}
)

var costEntryCPUCalculations = []struct {
	name         string
	entry        *CostTableEntry
	milliCPU     int64
	totalCPU     int64
	duration     time.Duration
	expectedCost int64
}{
	{
		name:         "half cpu for an hour",
		entry:        singleCPU32MebEntry,
		milliCPU:     500,
		duration:     time.Hour,
		expectedCost: 7500000,
	},
	{
		name:         "half cpu for 5 minutes",
		entry:        singleCPU32MebEntry,
		milliCPU:     500,
		duration:     time.Minute * 5,
		expectedCost: 625000,
	},
}

func TestCostEntryCPUCalculations(t *testing.T) {
	for _, tt := range costEntryCPUCalculations {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.entry.CPUCostMicroCents(float64(tt.milliCPU), tt.duration)
			if got != tt.expectedCost {
				t.Fatalf("expected cpu cost of %v got %v", tt.expectedCost, got)
			}
		})
	}
}

var costEntryMemoryCalculations = []struct {
	name         string
	entry        *CostTableEntry
	mib          int64
	totalMib     int64
	duration     time.Duration
	expectedCost int64
}{
	{
		name:         "mebibyte of memory for an hour",
		entry:        singleCPU32MebEntry,
		mib:          1048576,
		duration:     time.Hour,
		expectedCost: 1048576,
	},
	{
		name:         "mebibyte of memory for a minute",
		entry:        singleCPU32MebEntry,
		mib:          1048576,
		duration:     time.Minute,
		expectedCost: 17476,
	},
}

func TestCostEntryMemoryCalculations(t *testing.T) {
	for _, tt := range costEntryMemoryCalculations {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.entry.MemoryCostMicroCents(float64(tt.mib), tt.duration)
			if got != tt.expectedCost {
				t.Fatalf("expected memory cost of %v got %v", tt.expectedCost, got)
			}
		})
	}
}
