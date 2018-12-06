package coster

import (
	"testing"
	"time"

	"github.com/go-test/deep"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const strategyTestNodeName = "strategy-test-node"

var strategyTestNodeLabels = map[string]string{
	"test": "strategy",
}

var (
	testStrategyPodA = &core_v1.Pod{
		Spec: core_v1.PodSpec{
			NodeName: strategyTestNodeName,
			Containers: []core_v1.Container{
				core_v1.Container{
					Resources: core_v1.ResourceRequirements{
						Requests: core_v1.ResourceList{
							"cpu":    resource.MustParse("500m"),
							"memory": resource.MustParse("32Mi"),
						},
					},
				},
			},
		},
	}
	testStrategyPodB = &core_v1.Pod{
		Spec: core_v1.PodSpec{
			NodeName: strategyTestNodeName,
			Containers: []core_v1.Container{
				core_v1.Container{
					Resources: core_v1.ResourceRequirements{
						Requests: core_v1.ResourceList{
							"cpu":    resource.MustParse("250m"),
							"memory": resource.MustParse("32Mi"),
						},
					},
				},
			},
		},
	}
)

var testStrategyNode = &core_v1.Node{
	ObjectMeta: metav1.ObjectMeta{
		Name:   strategyTestNodeName,
		Labels: strategyTestNodeLabels,
	},
	Status: core_v1.NodeStatus{
		Capacity: core_v1.ResourceList{
			"cpu":    resource.MustParse("1"),
			"memory": resource.MustParse("1Gi"),
		},
	},
}

var testStrategyCostTable = CostTable{
	Entries: []*CostTableEntry{
		&CostTableEntry{
			Labels: strategyTestNodeLabels,
			HourlyMilliCPUCostMicroCents:   1000,
			HourlyMemoryByteCostMicroCents: 1,
		},
	},
}

var testCPUStrategyCases = []struct {
	name              string
	pods              []*core_v1.Pod
	nodes             []*core_v1.Node
	table             CostTable
	duration          time.Duration
	strategy          PricingStrategy
	expectedCostItems []CostItem
}{
	{
		name:     "Happy day CPUPricingStrategy with a single pod.",
		pods:     []*core_v1.Pod{testStrategyPodA},
		nodes:    []*core_v1.Node{testStrategyNode},
		table:    testStrategyCostTable,
		duration: time.Hour,
		strategy: CPUPricingStrategy,
		expectedCostItems: []CostItem{
			CostItem{
				Value:    500000,
				Kind:     ResourceCostCPU,
				Pod:      testStrategyPodA,
				Node:     testStrategyNode,
				Strategy: StrategyNameCPU,
			},
		},
	},
	{
		name:     "Happy day CPUPricingStrategy with two pods.",
		pods:     []*core_v1.Pod{testStrategyPodA, testStrategyPodB},
		nodes:    []*core_v1.Node{testStrategyNode},
		table:    testStrategyCostTable,
		duration: time.Hour,
		strategy: CPUPricingStrategy,
		expectedCostItems: []CostItem{
			CostItem{
				Value:    500000,
				Kind:     ResourceCostCPU,
				Pod:      testStrategyPodA,
				Node:     testStrategyNode,
				Strategy: StrategyNameCPU,
			},
			CostItem{
				Value:    250000,
				Kind:     ResourceCostCPU,
				Pod:      testStrategyPodB,
				Node:     testStrategyNode,
				Strategy: StrategyNameCPU,
			},
		},
	},
	{
		name:     "Happy day WeightedPricingStrategy with two pods.",
		pods:     []*core_v1.Pod{testStrategyPodA, testStrategyPodB},
		nodes:    []*core_v1.Node{testStrategyNode},
		table:    testStrategyCostTable,
		duration: time.Hour,
		strategy: WeightedPricingStrategy,
		expectedCostItems: []CostItem{
			CostItem{
				Value:    537537578,
				Kind:     ResourceCostWeighted,
				Pod:      testStrategyPodA,
				Node:     testStrategyNode,
				Strategy: StrategyNameWeighted,
			},
			CostItem{
				Value:    537204245,
				Kind:     ResourceCostWeighted,
				Pod:      testStrategyPodB,
				Node:     testStrategyNode,
				Strategy: StrategyNameWeighted,
			},
		},
	},
	{
		name:     "Happy day NodePricingStrategy.",
		pods:     []*core_v1.Pod{},
		nodes:    []*core_v1.Node{testStrategyNode},
		table:    testStrategyCostTable,
		duration: time.Hour,
		strategy: NodePricingStrategy,
		expectedCostItems: []CostItem{
			CostItem{
				Value:    1074741824, // 1073741824 (gibibyte) + 1e6 (1000 millicpus * 1000 per millicpu hour)
				Kind:     ResourceCostNode,
				Node:     testStrategyNode,
				Strategy: StrategyNameNode,
			},
		},
	},
}

func TestCPUStrategyCalculations(t *testing.T) {
	for _, tt := range testCPUStrategyCases {
		t.Run(tt.name, func(t *testing.T) {
			ci := tt.strategy.Calculate(tt.table, tt.duration, tt.pods, tt.nodes)
			if diff := deep.Equal(ci, tt.expectedCostItems); diff != nil {
				t.Fatal(diff)
			}
		})
	}
}
