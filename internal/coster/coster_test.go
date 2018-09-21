package coster

import (
	"reflect"
	"testing"
	"time"

	"go.opencensus.io/exporter/prometheus"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"

	"github.com/jacobstr/kostanza/internal/lister"
)

var resourceTestPod core_v1.Pod = core_v1.Pod{
	Spec: core_v1.PodSpec{
		Containers: []core_v1.Container{
			core_v1.Container{
				Resources: core_v1.ResourceRequirements{
					Requests: core_v1.ResourceList{
						"memory": resource.MustParse("32Mi"),
						"cpu":    resource.MustParse("1000m"),
					},
				},
			},
			core_v1.Container{
				Resources: core_v1.ResourceRequirements{
					Requests: core_v1.ResourceList{
						"memory": resource.MustParse("16Mi"),
						"cpu":    resource.MustParse("500m"),
					},
				},
			},
		},
	},
}

var sumPodResourceCases = []struct {
	name          string
	kind          core_v1.ResourceName
	pod           core_v1.Pod
	expectedValue int64
}{
	{
		name:          "sum by memory",
		kind:          core_v1.ResourceMemory,
		pod:           resourceTestPod,
		expectedValue: 50331648,
	},
	{
		name:          "sum by cpu",
		kind:          core_v1.ResourceCPU,
		pod:           resourceTestPod,
		expectedValue: 1500,
	},
}

func TestSumPodResources(t *testing.T) {
	for _, tt := range sumPodResourceCases {
		t.Run(tt.name, func(t *testing.T) {
			got := sumPodResource(&tt.pod, tt.kind)
			if got != tt.expectedValue {
				t.Fatalf("expected resource sum of %#v but got %#v", tt.expectedValue, got)
			}
		})
	}
}

func TestNewKubernetesCoster(t *testing.T) {
	dur := time.Hour
	cfg := &Config{}
	cli := testclient.NewSimpleClientset()
	lis := ":5000"
	pro, err := prometheus.NewExporter(prometheus.Options{})
	if err != nil {
		t.Fatalf("could not get prometheus exporter %v", err)
	}

	c, err := NewKubernetesCoster(dur, cfg, cli, pro, lis)
	if err != nil {
		t.Fatalf("error constructing coster: %v", err)
	}
	if c.podLister == nil {
		t.Fatal("constructor should populate pod lister")
	}

	if c.nodeLister == nil {
		t.Fatal("constructor should populate node lister")
	}

}

const calculateTestNodeName = "woot"

var calculateTestNodeLabels = map[string]string{
	"test": "test",
}

var testCalculationPod *core_v1.Pod = &core_v1.Pod{
	Spec: core_v1.PodSpec{
		NodeName: calculateTestNodeName,
		Containers: []core_v1.Container{
			core_v1.Container{
				Resources: core_v1.ResourceRequirements{
					Requests: core_v1.ResourceList{
						"cpu": resource.MustParse("500m"),
					},
				},
			},
		},
	},
}

var testCalculationNode *core_v1.Node = &core_v1.Node{
	ObjectMeta: metav1.ObjectMeta{
		Name:   calculateTestNodeName,
		Labels: calculateTestNodeLabels,
	},
}

var calculcateCases = []struct {
	name              string
	pods              []*core_v1.Pod
	nodes             []*core_v1.Node
	config            *Config
	expectedCostItems []podCostItem
}{
	{
		name:  "single container pod on a node using half it's cpu",
		pods:  []*core_v1.Pod{testCalculationPod},
		nodes: []*core_v1.Node{testCalculationNode},
		config: &Config{
			Pricing: CostTable{
				Entries: []*CostTableEntry{
					&CostTableEntry{
						Labels:               calculateTestNodeLabels,
						TotalMilliCPU:        1000,
						TotalMemoryBytes:     1024,
						HourlyCostMicroCents: 1000000,
					},
				},
			},
		},
		expectedCostItems: []podCostItem{
			podCostItem{
				value: 500000,
				kind:  ResourceCostCPU,
				pod:   testCalculationPod,
			},
		},
	},
}

func TestCalculate(t *testing.T) {
	for _, tt := range calculcateCases {
		t.Run(tt.name, func(t *testing.T) {
			pro, err := prometheus.NewExporter(prometheus.Options{})
			if err != nil {
				t.Fatalf("could not get prometheus exporter %v", err)
			}

			nodl := lister.FakeNodeLister{Nodes: tt.nodes}
			podl := lister.FakePodLister{Pods: tt.pods}

			c := &coster{
				interval:   time.Hour,
				exporter:   pro,
				listenAddr: ":5000",
				nodeLister: &nodl,
				podLister:  &podl,
				config:     tt.config,
			}

			ci, err := c.calculate()
			if err != nil {
				t.Fatalf("unexpected error calculation costs: %v", err)
			}

			if !reflect.DeepEqual(ci, tt.expectedCostItems) {
				t.Fatalf("wanted cost items %#v, got %#v", tt.expectedCostItems, ci)
			}
		})
	}
}