package coster

import (
	"time"

	"github.com/jacobstr/kostanza/internal/log"
	"go.uber.org/zap"
	core_v1 "k8s.io/api/core/v1"
)

const (
	// StrategyNameCPU is used whenever we derive a cost metric using the CPUPricingStrategy.
	StrategyNameCPU = "CPUPricingStrategy"
	// StrategyNameNode is used whenever we derive a cost metric using the NodePricingStrategy.
	StrategyNameNode = "NodePricingStrategy"
	// StrategyNameWeighted is used whenever we derive a cost metric using the WeightedPricingStrategy.
	StrategyNameWeighted = "WeightedPricingStrategy"
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
	cpu    int64
	memory int64
	node   *core_v1.Node
}

// CPUPricingStrategy calculates the cost of a pod based strictly on it's share
// of CPU requests as a fraction of all CPU requests on the node onto which it
// is allocated. The pods and nodes provided are expected to represent all pods
// and nodes that we care about and should generally exclude DaemonSet pods that
// are scheduled to the node by cluster administrators.
var CPUPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nrm := buildNodeResourceMap(pods, nodes)
	cis := []CostItem{}
	for _, p := range pods {
		cpu := sumPodResource(p, core_v1.ResourceCPU)
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

		ci := CostItem{
			Kind:     ResourceCostCPU,
			Value:    te.CPUCostMicroCents(float64(cpu)/float64(nr.cpu), duration),
			Pod:      p,
			Node:     nr.node,
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

// WeightedPricingStrategy calculates the cost of a pod based on it's average use of the
// CPU and Memory requests as a fraction of all CPU and memory requests on the node onto
// which it has been allocated.
var WeightedPricingStrategy = PricingStrategyFunc(func(table CostTable, duration time.Duration, pods []*core_v1.Pod, nodes []*core_v1.Node) []CostItem {
	nrm := buildNodeResourceMap(pods, nodes)
	cis := []CostItem{}
	for _, p := range pods {
		cpu := sumPodResource(p, core_v1.ResourceCPU)
		mem := sumPodResource(p, core_v1.ResourceMemory)

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

		cpucost := te.CPUCostMicroCents(float64(cpu)/float64(nr.cpu), duration)
		memcost := te.MemoryCostMicroCents(float64(mem)/float64(nr.memory), duration)

		ci := CostItem{
			Kind:     ResourceCostWeighted,
			Value:    (cpucost + memcost) / 2,
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
		ci := CostItem{
			Kind:     ResourceCostNode,
			Value:    te.CPUCostMicroCents(1, duration),
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
func sumPodResource(p *core_v1.Pod, kind core_v1.ResourceName) int64 {
	total := int64(0)
	for _, c := range p.Spec.Containers {
		res, ok := c.Resources.Requests[kind]
		if ok {
			total = total + (&res).MilliValue()
		}
	}

	// The millivalue for memory is given in thousandths of bytes, which is a goofy
	// number for anyone calling this function.
	if kind == core_v1.ResourceMemory {
		return total / 1000
	}
	return total
}

type nodeResourceMap map[string]allocatedNodeResources

func buildNodeResourceMap(pods []*core_v1.Pod, nodes []*core_v1.Node) nodeResourceMap {
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
		nr.cpu += sumPodResource(p, core_v1.ResourceCPU)
		nr.memory += sumPodResource(p, core_v1.ResourceMemory)
		nrm[p.Spec.NodeName] = nr
	}

	// For caller's sake, ensure we do not divide by zero by setting cpu / memory
	// to the alloctable node resources if our calculated cpu and memory remain at
	// 0 after we've summed pod resource allocations for the pods running on all
	// nodes. This may happen if pods lack resource requests, which is less than
	// ideal for our purposes since scheduling and subsequently autoscaling requires
	// these in order to behave in a well-defined manner.
	for k, v := range nrm {
		if v.cpu == 0 {
			log.Log.Warnw(
				"node has no cpu resource requests from allocated pods, defaulting to alloctable capacity.",
				zap.String("nodeName", v.node.ObjectMeta.Name),
			)
			c := v.node.Status.Allocatable.Cpu()
			if c != nil {
				v.cpu = c.MilliValue()
			}
		}
		if v.memory == 0 {
			log.Log.Warnw(
				"node has no memory resource requests from allocated pods, defaulting to allocatable capacity.",
				zap.String("nodeName", v.node.ObjectMeta.Name),
			)
			m := v.node.Status.Allocatable.Memory()
			if m != nil {
				v.memory = m.MilliValue() / 1000
			}
		}
		nrm[k] = v
	}

	return nrm
}
