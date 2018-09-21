package coster

import (
	"time"

	"github.com/pkg/errors"
)

var (
	// ErrNoCostEntry is returned when we cannot find a suitable CostEntry in a CostTable.
	ErrNoCostEntry = errors.New("could not find an appropriate cost entry")
)

// Labels augments a slice ofa labels with matching functionality.
type Labels map[string]string

// Match checks if the provided label exists within the available labels.
func (l Labels) Match(key, value string) bool {
	for k, v := range l {
		if k == key && v == value {
			return true
		}
	}
	return false
}

// CostTableEntry models the cost of a nodes resources. The labels are used to
// identify nodes.
type CostTableEntry struct {
	Labels               Labels
	TotalMilliCPU        int64
	TotalMemoryBytes     int64
	HourlyCostMicroCents int64
}

// Match returns true if all of the CostTableEntry's labels match some subeset
// of the labels provided.
//
// Additional labels can be used to increase the specificity of the selector and
// are generally useful for refining cost table configurations - e.g. from
// global, to per region pricing by using labels. For example, in GCP the following
// labels may be added to nodes in Kubernetes 1.11:
// - beta.kubernetes.io/instance-type: n1-standard-16
// - failure-domain.beta.kubernetes.io/region: us-central1
// - failure-domain.beta.kubernetes.io/zone: us-central1-b
//
// Note: A special case of match against an empty list of labels will always match
// a CostTableEntry with no Labels.
func (e *CostTableEntry) Match(labels Labels) bool {
	if len(labels) == 0 && len(e.Labels) == 0 {
		return true
	}

	for k, v := range e.Labels {
		if !labels.Match(k, v) {
			return false
		}
	}
	return true
}

// CPUCostMicroCents returns the cost of the provided millicpu over a given duration in millionths of a cent.
func (e *CostTableEntry) CPUCostMicroCents(mcpu int64, duration time.Duration) int64 {
	cpufrac := float64(mcpu) / float64(e.TotalMilliCPU)
	durfrac := float64(duration) / float64(time.Hour)
	return int64(cpufrac * durfrac * float64(e.HourlyCostMicroCents))
}

// MemoryCostMicroCents returns the cost of the provided memory over a given duration in millionths of a cent.
func (e *CostTableEntry) MemoryCostMicroCents(mib int64, duration time.Duration) int64 {
	memfrac := float64(mib) / float64(e.TotalMemoryBytes)
	durfrac := float64(duration) / float64(time.Hour)
	return int64(memfrac * durfrac * float64(e.HourlyCostMicroCents))
}

// CostTable is a collection of CostTableEntries, generally used to look up pricing
// data via a set of labels provided callers of it's FindByLabels method.
// The order of of entries determines precedence of potentially multiple
// applicable matches.
type CostTable struct {
	Entries []*CostTableEntry
}

// FindByLabels returns the first matching CostTableEntry whose labels
// are a subset of those provided.
//
// A CostTableEntry with labels:
// 	{"size": "large", "region": usa"}
//
// will match:
// 	{"size": "large", "region": "usa"}
// an will also match:
// 	{"size": "large", "region": "usa", "foo": "bar"}
//
// but will not match:
// 	{"region": "usa"}
func (ct *CostTable) FindByLabels(labels Labels) (*CostTableEntry, error) {
	for _, e := range ct.Entries {
		if e.Match(labels) {
			return e, nil
		}
	}
	return nil, ErrNoCostEntry
}