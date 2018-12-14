// Copyright 2018 Planet Labs Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-test/deep"
	"go.opencensus.io/exporter/prometheus"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"

	"github.com/jacobstr/kostanza/internal/lister"
)

var resourceTestPod = core_v1.Pod{
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

	c, err := NewKubernetesCoster(dur, cfg, cli, pro, lis, nil)
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

var testCalculationPod = &core_v1.Pod{
	Spec: core_v1.PodSpec{
		NodeName: calculateTestNodeName,
		Containers: []core_v1.Container{
			core_v1.Container{
				Resources: core_v1.ResourceRequirements{
					Requests: core_v1.ResourceList{
						"cpu": resource.MustParse("1000m"),
					},
				},
			},
		},
	},
}

var testCalculationNode = &core_v1.Node{
	ObjectMeta: metav1.ObjectMeta{
		Name:   calculateTestNodeName,
		Labels: calculateTestNodeLabels,
	},
}

var calculateCases = []struct {
	name              string
	pods              []*core_v1.Pod
	nodes             []*core_v1.Node
	config            *Config
	expectedCostItems []CostItem
}{
	{
		name:  "single container pod on a node using a single cpu",
		pods:  []*core_v1.Pod{testCalculationPod},
		nodes: []*core_v1.Node{testCalculationNode},
		config: &Config{
			Pricing: CostTable{
				Entries: []*CostTableEntry{
					&CostTableEntry{
						Labels: calculateTestNodeLabels,
						HourlyMilliCPUCostMicroCents: 1000,
					},
				},
			},
		},
		expectedCostItems: []CostItem{
			CostItem{
				Value:    1000000,
				Kind:     ResourceCostCPU,
				Pod:      testCalculationPod,
				Node:     testCalculationNode,
				Strategy: StrategyNameCPU,
			},
		},
	},
}

func TestCalculate(t *testing.T) {
	for _, tt := range calculateCases {
		t.Run(tt.name, func(t *testing.T) {
			pro, err := prometheus.NewExporter(prometheus.Options{})
			if err != nil {
				t.Fatalf("could not get prometheus exporter %v", err)
			}

			nodl := lister.FakeNodeLister{Nodes: tt.nodes}
			podl := lister.FakePodLister{Pods: tt.pods}

			c := &coster{
				interval:           time.Hour,
				ticker:             time.NewTicker(time.Hour),
				prometheusExporter: pro,
				listenAddr:         ":5000",
				nodeLister:         &nodl,
				podLister:          &podl,
				config:             tt.config,
				strategies:         []PricingStrategy{CPUPricingStrategy},
			}

			ci, err := c.calculate()
			if err != nil {
				t.Fatalf("unexpected error calculation costs: %v", err)
			}

			if diff := deep.Equal(ci, tt.expectedCostItems); diff != nil {
				t.Fatal(diff)
			}
		})
	}
}

func TestRun(t *testing.T) {
	pro, err := prometheus.NewExporter(prometheus.Options{})
	if err != nil {
		t.Fatalf("could not get prometheus exporter %v", err)
	}

	nodl := lister.FakeNodeLister{Nodes: []*core_v1.Node{}}
	podl := lister.FakePodLister{Pods: []*core_v1.Pod{}}

	c := &coster{
		interval:           time.Hour,
		ticker:             time.NewTicker(time.Hour),
		prometheusExporter: pro,
		listenAddr:         ":5000",
		nodeLister:         &nodl,
		podLister:          &podl,
		strategies:         []PricingStrategy{},
	}

	ch := make(chan struct{})
	ctx, done := context.WithCancel(context.Background())
	go func() {
		defer close(ch)
		if err := c.Run(ctx); err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
	}()
	go func() {
		time.Sleep(time.Millisecond)
		done()
	}()
	<-ch
}
