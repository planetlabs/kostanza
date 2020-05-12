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
	"time"

	"github.com/planetlabs/kostanza/internal/log"
	"go.uber.org/zap"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// StrategyNameCPU is used whenever we derive a cost metric using the CPUPricingStrategy.
	StrategyNameCPU = "CPUPricingStrategy"
	// StrategyNameMemory is used whenever we derive a cost metric using the MemoryPricingStrategy.
	StrategyNameMemory = "MemoryPricingStrategy"
	// StrategyNameNode is used whenever we derive a cost metric using the NodePricingStrategy.
	StrategyNameNode = "NodePricingStrategy"
	// StrategyNameWeighted is used whenever we derive a cost metric using the WeightedPricingStrategy.
	StrategyNameWeighted = "WeightedPricingStrategy"
	// StrategyNameGPU is used whenever we derive a cost metric using the GPUPricingStrategy.
	StrategyNameGPU = "GPUPricingStrategy"
	// ResourceGPU is used for gpu resources, coinciding with modern versions of the nvidia-device-plugin.
	ResourceGPU = core_v1.ResourceName("nvidia.com/gpu")
)

// CostItem models the metadata associated with a pod and/or node cost.
// Generally, this is subsequently utilized in order to emit an associated cost
// metric with dimensions derived from an appropriately configured Mapper.
type CostItem struct {
	// The kind of cost figure represented.
	Kind ResourceCostKind
	// The strategy the yielded this CostItem.
	Strategy string
	// The value in microcents that it costs.
	Value int64
	// Kubernetes pod metadata associated with the pod which we're pricing out.
	Pod *core_v1.Pod
	// Kubernetes pod metadata associated with the node which we're pricing out.
	Node *core_v1.Node
}

// PricingStrategyFunc is an interface wrapper to convert a function into valid
// PricingStrategy.
type PricingStrategyFunc func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem

// Calculate returns CostItems given a pricing table of node costs, the duration
// we're costing out, and the pods as well as nodes running in a cluster.
func (f PricingStrategyFunc) Calculate(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	return f(table, duration, pods, nodes)
}

// PricingStrategy generates CostItems given the pods and nodes running in a cluster.
type PricingStrategy interface {
	Calculate(t CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem
}

// allocatedNodeResources tracks the allocated resources for a given node, generally determined by
// taking the sum of individual resource requests from pods.
type allocatedNodeResources struct {
	cpuUsed         int64
	memoryUsed      int64
	gpuUsed         int64
	cpuAvailable    int64
	gpuAvailable    int64
	memoryAvailable int64
	node            *core_v1.Node
}

func (nr allocatedNodeResources) CPUScale() float64 {
	if nr.cpuUsed == 0 {
		return 0
	}
	return float64(nr.cpuAvailable) / float64(nr.cpuUsed)
}

func (nr allocatedNodeResources) MemoryScale() float64 {
	if nr.memoryUsed == 0 {
		return 0
	}
	return float64(nr.memoryAvailable) / float64(nr.memoryUsed)
}

func (nr allocatedNodeResources) GPUScale() float64 {
	if nr.gpuUsed == 0 {
		return 0
	}
	return float64(nr.gpuAvailable) / float64(nr.gpuUsed)
}

// gpuCapacity mirrors the definitions of ResourceList.Memory and
// ResourceList.CPU in k8s client-go and provides equivalent functionality for
// GPU capacity.
func gpuCapacity(self *core_v1.ResourceList) *resource.Quantity {
	if val, ok := (*self)[ResourceGPU]; ok {
		return &val
	}
	return &resource.Quantity{Format: resource.DecimalSI}
}

// CPUPricingStrategy calculates the cost of a pod based strictly on it's share
// of CPU requests as a fraction of all CPU available on the node onto which it
// is allocated.
var CPUPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nm := buildNodeMap(nodes)
	cis := []CostItem{}
	for _, p := range pods {
		cpu := sumPodResource(p, core_v1.ResourceCPU)
		node, ok := nm[p.Spec.NodeName]
		if !ok {
			log.Log.Warnw("could not find nodeResourceMap for node", zap.String("nodeName", p.Spec.NodeName))
			continue
		}

		te, err := table.FindByLabels(node.Labels)
		if err != nil {
			log.Log.Warnw("could not find pricing entry for node", zap.String("nodeName", node.ObjectMeta.Name))
			continue
		}

		ci := CostItem{
			Kind:     ResourceCostCPU,
			Value:    te.CPUCostMicroCents(float64(cpu), duration),
			Pod:      p,
			Node:     node,
			Strategy: StrategyNameCPU,
		}
		log.Log.Debugw(
			"generated cost item",
			zap.String("pod", ci.Pod.ObjectMeta.Name),
			zap.String("strategy", ci.Strategy),
			zap.Int64("value", ci.Value),
		)
		cis = append(cis, ci)
	}
	return cis
})

// MemoryPricingStrategy calculates the cost of a pod based strictly on it's
// share of memory requests as a fraction of all memory on the node onto which
// it was scheduled.
var MemoryPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nm := buildNodeMap(nodes)
	cis := []CostItem{}
	for _, p := range pods {
		mem := sumPodResource(p, core_v1.ResourceMemory)
		node, ok := nm[p.Spec.NodeName]
		if !ok {
			log.Log.Warnw("could not find nodeResourceMap for node", zap.String("nodeName", p.Spec.NodeName))
			continue
		}

		te, err := table.FindByLabels(node.Labels)
		if err != nil {
			log.Log.Warnw("could not find pricing entry for node", zap.String("nodeName", node.ObjectMeta.Name))
			continue
		}

		ci := CostItem{
			Kind:     ResourceCostMemory,
			Value:    te.MemoryCostMicroCents(float64(mem), duration),
			Pod:      p,
			Node:     node,
			Strategy: StrategyNameMemory,
		}
		log.Log.Debugw(
			"generated cost item",
			zap.String("pod", ci.Pod.ObjectMeta.Name),
			zap.String("strategy", ci.Strategy),
			zap.Int64("value", ci.Value),
		)
		cis = append(cis, ci)
	}
	return cis
})

// GPUPricingStrategy generates cost metrics that account for the cost of GPUs consumed by pods.
var GPUPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nm := buildNodeMap(nodes)
	cis := []CostItem{}
	for _, p := range pods {
		gpu := sumPodResource(p, ResourceGPU)
		node, ok := nm[p.Spec.NodeName]

		if gpu == 0 {
			log.Log.Debugw("skipping pod that does not utilize gpu", zap.String("pod", p.ObjectMeta.Name))
			continue
		}

		if !ok {
			log.Log.Warnw("could not find nodeResourceMap for node", zap.String("nodeName", p.Spec.NodeName))
			continue
		}

		te, err := table.FindByLabels(node.Labels)
		if err != nil {
			log.Log.Warnw("could not find pricing entry for node", zap.String("nodeName", node.ObjectMeta.Name))
			continue
		}

		ci := CostItem{
			Kind:     ResourceCostGPU,
			Value:    te.GPUCostMicroCents(float64(gpu), duration),
			Pod:      p,
			Node:     node,
			Strategy: StrategyNameGPU,
		}
		log.Log.Debugw(
			"generated cost item",
			zap.String("pod", ci.Pod.ObjectMeta.Name),
			zap.String("strategy", ci.Strategy),
			zap.Int64("value", ci.Value),
		)
		cis = append(cis, ci)
	}
	return cis
})

// WeightedPricingStrategy calculates the cost of a pod based on it's average use of the
// CPU and Memory requests as a fraction of all CPU and memory requests on the node onto
// which it has been allocated. This strategy ensures that unallocated resources do not
// go unattributed and has a tendency to punish pods that may occupy oddly shaped resources
// or those that frequently churn.
var WeightedPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nrm := buildNormalizedNodeResourceMap(pods, nodes)
	cis := []CostItem{}
	for _, p := range pods {
		cpu := sumPodResource(p, core_v1.ResourceCPU)
		mem := sumPodResource(p, core_v1.ResourceMemory)
		gpu := sumPodResource(p, ResourceGPU)

		nr, ok := nrm[p.Spec.NodeName]
		if !ok {
			log.Log.Warnw("could not find nodeResourceMap for node", zap.String("nodeName", p.Spec.NodeName))
			continue
		}

		te, err := table.FindByLabels(nr.node.Labels)
		if err != nil {
			log.Log.Warnw("could not find pricing entry for node", zap.String("nodeName", nr.node.ObjectMeta.Name))
			continue
		}

		// We "normalize" cpu, memory, and gpu utilization by scaling the utilized resources
		// of pods by the global utilization of the respective resource on the node.
		cpucost := te.CPUCostMicroCents(float64(cpu)*nr.CPUScale(), duration)
		memcost := te.MemoryCostMicroCents(float64(mem)*nr.MemoryScale(), duration)
		gpucost := te.GPUCostMicroCents(float64(gpu)*nr.GPUScale(), duration)

		ci := CostItem{
			Kind:     ResourceCostWeighted,
			Value:    cpucost + memcost + gpucost,
			Pod:      p,
			Node:     nr.node,
			Strategy: StrategyNameWeighted,
		}
		log.Log.Debugw(
			"generated cost item",
			zap.String("pod", ci.Pod.ObjectMeta.Name),
			zap.String("strategy", ci.Strategy),
			zap.Int64("value", ci.Value),
		)
		cis = append(cis, ci)
	}
	return cis
})

// NodePricingStrategy generates cost metrics that represent the cost of an
// active node, regardless of pod. This is generally used to provide an overall
// cost metric that can be compared to per-pod costs.
var NodePricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	cis := []CostItem{}
	for _, n := range nodes {
		te, err := table.FindByLabels(n.Labels)
		if err != nil {
			log.Log.Warnw("could not find pricing entry for node", zap.String("nodeName", n.ObjectMeta.Name))
			continue
		}

		c := n.Status.Capacity.Cpu()
		if c == nil {
			log.Log.Warnw("could not get node cpu capacity, skipping", zap.String("nodeName", n.ObjectMeta.Name))
			continue
		}

		m := n.Status.Capacity.Memory()
		if m == nil {
			log.Log.Warnw("could not get node memory capacity, skipping", zap.String("nodeName", n.ObjectMeta.Name))
			continue
		}

		memcost := te.MemoryCostMicroCents(float64(m.MilliValue())/1000, duration)
		cpucost := te.CPUCostMicroCents(float64(c.MilliValue()), duration)

		gpucost := int64(0)
		if g := gpuCapacity(&n.Status.Capacity); g != nil {
			gpucost = te.GPUCostMicroCents(float64(g.Value()), duration)
		}

		ci := CostItem{
			Kind:     ResourceCostNode,
			Value:    memcost + cpucost + gpucost,
			Node:     n,
			Strategy: StrategyNameNode,
		}
		log.Log.Debugw(
			"generated cost item",
			zap.String("node", ci.Node.ObjectMeta.Name),
			zap.String("strategy", ci.Strategy),
			zap.Int64("value", ci.Value),
		)
		cis = append(cis, ci)
	}
	return cis
})

// sumPodResource calculates the total resource requests of `kind` for all
// containers within a given Pod. The meaning of the value returned depends on
// the kind chosen:
// 	- cpu: The number of millicpus. 1 cpu is 1000.
//  - memory: The number of bytes.
//  - nvidia.com/gpu: The number of gpu units regardless of model.
func sumPodResource(p *core_v1.Pod, kind core_v1.ResourceName) int64 {
	total := int64(0)
	for _, c := range p.Spec.Containers {
		res, ok := c.Resources.Requests[kind]
		if !ok {
			continue
		}

		if kind == core_v1.ResourceMemory {
			total = total + (&res).Value()
		} else if kind == ResourceGPU {
			total = total + (&res).Value()
		} else {
			total = total + (&res).MilliValue()
		}
	}

	return total
}

type nodeResourceMap map[string]allocatedNodeResources
type nodeMap map[string]*core_v1.Node

func buildNodeMap(nodes []*core_v1.Node) nodeMap {
	nm := nodeMap{}
	for _, n := range nodes {
		nm[n.ObjectMeta.Name] = n
	}
	return nm
}

// cpu and memory models just need to take the pod resources and multiply them by the hourly cost.
// normalized models need to take the pod resources and scale them by the unutlized fraction
// e.g. my pod uses 500 cpu
// the node has 1 cpu
// my pod is the only pod on the node, and total nod resources are 500
func buildNormalizedNodeResourceMap(pods []*core_v1.Pod, nodes []*core_v1.Node) nodeResourceMap { // nolint: gocyclo
	nrm := nodeResourceMap{}

	for _, n := range nodes {
		nrm[n.ObjectMeta.Name] = allocatedNodeResources{node: n}
	}

	// We sum the total allocated resources on every node from our list of pods.
	// Some strategies may wish to price pods based on their fraction of allocated
	// node resources, rather than the total resources available on a node. This
	// may punish lone pods that are initially scheduled onto large nodes, but this
	// may be desirable as it rightfully punishes applications that may cause
	// frequent node turnover.
	for _, p := range pods {
		nr, ok := nrm[p.Spec.NodeName]
		if !ok {
			log.Log.Warnw("unexpected missing node from NodeMap", zap.String("nodeName", p.Spec.NodeName))
			continue
		}
		nr.cpuUsed += sumPodResource(p, core_v1.ResourceCPU)
		nr.memoryUsed += sumPodResource(p, core_v1.ResourceMemory)
		nr.gpuUsed += sumPodResource(p, ResourceGPU)
		nrm[p.Spec.NodeName] = nr
	}

	for k, v := range nrm {
		c := v.node.Status.Capacity.Cpu()
		if c != nil {
			v.cpuAvailable = c.MilliValue()
		}

		m := v.node.Status.Capacity.Memory()
		if m != nil {
			v.memoryAvailable = m.Value()
		}

		g := gpuCapacity(&v.node.Status.Capacity)
		if g != nil {
			v.gpuAvailable = g.Value()
		}

		// The ratio of cpuUsed / cpuAvailable is used for proportional scaling of
		// resources to "normalize" pod resource utilization to a full node. If
		// cpuUsed is 0 because the pods that are running have not made resource
		// requests, there's a possible divide by 0 in calling code so we default to
		// setting cpuUsed to cpuAvailable.
		if v.cpuUsed == 0 {
			v.cpuUsed = v.cpuAvailable
		}

		if v.memoryUsed == 0 {
			v.memoryUsed = v.memoryAvailable
		}

		if v.gpuUsed == 0 {
			v.gpuUsed = v.gpuAvailable
		}

		nrm[k] = v
	}

	return nrm
}
