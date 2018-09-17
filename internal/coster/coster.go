package coster

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/jacobstr/kostanza/internal/lister"
	"github.com/jacobstr/kostanza/internal/log"
)

const (
	UnknownService   = "unknown"
	UnknownComponent = "unknown"
)

type ResourceCostKind string

var (
	ResourceCostCPU    = ResourceCostKind("cpu")
	ResourceCostMemory = ResourceCostKind("memory")
)

var (
	// ErrNoPodNode may be returned during races where the pod containing a given
	// node has disappeared.
	ErrNoPodNode = errors.New("could not lookup node for pod")
)

var (
	// MeasureCost is the stat for tracking costs in millionths of a cent.
	MeasureCost = stats.Int64("kostanza/measures/cost", "Cost in millionths of a cent", "µ¢")

	// TagKind is kind of cost, for example: cpu, memory, network-egress.
	TagKind, _ = tag.NewKey("kind")
)

type Coster interface {
	CalculateAndEmit() error
	Run(ctx context.Context) error
}

type Config struct {
	Mapper  Mapper
	Pricing CostTable
}

// NewKubernetesCoster returns a new coster that talks to a kubernetes cluster
// via the provided client.
func NewKubernetesCoster(interval time.Duration, config *Config, client kubernetes.Interface, exporter *prometheus.Exporter, listenAddr string) (*coster, error) {
	podLister := lister.NewKubernetesPodLister(client)
	nodeLister := lister.NewKubernetesNodeLister(client)

	if config == nil {
		return nil, errors.New("coster configuration is required")
	}

	return &coster{
		interval:   interval,
		ticker:     time.NewTicker(interval),
		podLister:  podLister,
		nodeLister: nodeLister,
		config:     config,
		exporter:   exporter,
		listenAddr: listenAddr,
	}, nil
}

type coster struct {
	interval   time.Duration
	ticker     *time.Ticker
	podLister  lister.PodLister
	nodeLister lister.NodeLister
	config     *Config
	exporter   *prometheus.Exporter
	listenAddr string
}

// podCostItem models cost metadata for a pod.
type podCostItem struct {
	// The kind of cost figure represented.
	kind ResourceCostKind
	// The value in microcents that it costs.
	value int64
	// Kubernetes pod metadata associated with the pod.
	pod *core_v1.Pod
}

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
	if kind == "memory" {
		return total / 1000
	}
	return total
}

func costItemForPod(pod *core_v1.Pod, pricing *CostTable, interval time.Duration, nodeMap map[string]*core_v1.Node) (*podCostItem, error) {
	c := sumPodResource(pod, core_v1.ResourceCPU)
	n, ok := nodeMap[pod.Spec.NodeName]
	if !ok {
		log.Log.Warnw(
			"node lookup for pod failed - the node may have gone away",
			zap.String("nodeName", pod.Spec.NodeName),
			zap.String("pod", pod.ObjectMeta.Name),
		)
		return nil, ErrNoPodNode
	}

	ce, err := pricing.FindByLabels(n.Labels)
	if err != nil {
		log.Log.Errorw(
			"could not find suitable pricing table entry",
			zap.String("pod", pod.ObjectMeta.Name),
		)
		return nil, err
	}

	ci := podCostItem{
		pod:   pod,
		kind:  ResourceCostCPU,
		value: ce.CPUCostMicroCents(c, interval),
	}

	return &ci, nil
}

// Calculate returns a slice of podCostItem records that expose
// pricing details for services.
func (c *coster) calculate() ([]podCostItem, error) {
	log.Log.Debug("cost calculation loop triggered")

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	nodeMap := map[string]*core_v1.Node{}
	for _, n := range nodes {
		nodeMap[n.ObjectMeta.Name] = n
	}

	costItems := []podCostItem{}
	for _, p := range pods {
		pc, err := costItemForPod(p, &c.config.Pricing, c.interval, nodeMap)
		if err != nil {
			log.Log.Errorw("could not calculated pod cost", zap.Error(err))
			continue
		}
		log.Log.Debugw(
			"calculated pod cost",
			zap.String("name", pc.pod.Name),
			zap.String("namespace", pc.pod.Namespace),
			zap.String("kind", string(pc.kind)),
			zap.Int64("value", pc.value),
		)
		costItems = append(costItems, *pc)
	}

	return costItems, nil
}

func (c *coster) CalculateAndEmit() error {
	costs, err := c.calculate()
	if err != nil {
		log.Log.Error("failed to calculate pod costs")
		return err
	}

	mp := &c.config.Mapper
	for _, ci := range costs {
		ctx, err := mp.MapContext(context.Background(), ci.pod)
		if err != nil {
			log.Log.Errorw("could not update tag context from pod metadata", zap.Error(err))
		}

		ctx, err = tag.New(ctx, tag.Upsert(TagKind, string(ci.kind)))
		if err != nil {
			log.Log.Errorw("could not update tag context with kind", zap.Error(err))
		}
		stats.Record(ctx, MeasureCost.M(ci.value))
	}

	return nil
}

func (c *coster) Run(ctx context.Context) error {
	ctx, done := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer done()
		return c.podLister.Run(ctx.Done())
	})

	g.Go(func() error {
		defer done()
		return c.nodeLister.Run(ctx.Done())
	})

	g.Go(func() error {
		defer done()

		mux := http.NewServeMux()
		mux.Handle("/metrics", c.exporter)

		s := http.Server{
			Addr:    c.listenAddr,
			Handler: mux,
		}
		log.Log.Info("starting server on %s", c.listenAddr)

		go func() {
			<-ctx.Done()
			s.Shutdown(ctx)
		}()

		err := s.ListenAndServe()
		if err != nil {
			log.Log.Errorw("error listening", zap.Error(err))
			return err
		}
		return nil
	})

	g.Go(func() error {
		defer done()

		log.Log.Debug("starting cost calculation loop")
		defer log.Log.Debug("exiting cost calculation loop")

		for {
			select {
			case <-c.ticker.C:
				c.CalculateAndEmit()
			case <-ctx.Done():
				return nil
			}
		}
	})

	return g.Wait()
}

// NewConfigFromReader constructs a Config from an io.Reader.
func NewConfigFromReader(reader io.Reader) (*Config, error) {
	var c Config
	if err := json.NewDecoder(reader).Decode(&c); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal configuration")
	}

	return &c, nil
}
