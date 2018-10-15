package coster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/jacobstr/kostanza/internal/lister"
	"github.com/jacobstr/kostanza/internal/log"
)

// ResourceCostKind is used to indidicate what resource a cost was derived from.
type ResourceCostKind string

var (
	// ResourceCostCPU is a cost metric derived from CPU utilization.
	ResourceCostCPU = ResourceCostKind("cpu")
	// ResourceCostMemory is a cost metric derived from memory utilization.
	ResourceCostMemory = ResourceCostKind("memory")
	// ResourceCostWeighted is a cost metric derived from a weighted average of memory and cpu utilization.
	ResourceCostWeighted = ResourceCostKind("weighted")
	// ResourceCostNode represents the overall cost of a node.
	ResourceCostNode = ResourceCostKind("node")
)

var (
	// ErrNoPodNode may be returned during races where the pod containing a given
	// node has disappeared.
	ErrNoPodNode = errors.New("could not lookup node for pod")
)

var (
	// MeasureCost is the stat for tracking costs in millionths of a cent.
	MeasureCost = stats.Int64("kostanza/measures/cost", "Cost in millionths of a cent", "µ¢")
)

// Coster is used to calculate and emit metrics for services and components
// running in a kubernetes cluster.
type Coster interface {
	CalculateAndEmit() error
	Run(ctx context.Context) error
}

// Config contains the configuration data necessary to drive the
// kubernetesCoster. It includes both mapping information to teach it how to
// derive metric dimensions from pod labels, as well as as a pricing table to
// instruct it how expensive nodes are.
type Config struct {
	Mapper  Mapper
	Pricing CostTable
}

// NewKubernetesCoster returns a new coster that talks to a kubernetes cluster
// via the provided client.
func NewKubernetesCoster(
	interval time.Duration,
	config *Config,
	client kubernetes.Interface,
	prometheusExporter *prometheus.Exporter,
	listenAddr string,
	costExporters []CostExporter,
) (*coster, error) { // nolint: golint

	podLister := lister.NewKubernetesPodLister(client)
	nodeLister := lister.NewKubernetesNodeLister(client)

	if config == nil {
		return nil, errors.New("coster configuration is required")
	}

	return &coster{
		interval:           interval,
		ticker:             time.NewTicker(interval),
		podLister:          podLister,
		nodeLister:         nodeLister,
		config:             config,
		prometheusExporter: prometheusExporter,
		costExporters:      costExporters,
		listenAddr:         listenAddr,
		strategies:         []PricingStrategy{WeightedPricingStrategy, NodePricingStrategy},
	}, nil
}

type coster struct {
	interval           time.Duration
	ticker             *time.Ticker
	podLister          lister.PodLister
	nodeLister         lister.NodeLister
	config             *Config
	strategies         []PricingStrategy
	listenAddr         string
	prometheusExporter *prometheus.Exporter
	costExporters      []CostExporter
}

// Calculate returns a slice of podCostItem records that expose
// pricing details for services.
func (c *coster) calculate() ([]CostItem, error) {
	log.Log.Debug("cost calculation loop triggered")

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	cis := []CostItem{}
	for _, s := range c.strategies {
		cis = append(cis, s.Calculate(c.config.Pricing, c.interval, pods, nodes)...)
	}
	return cis, nil
}

func (c *coster) CalculateAndEmit() error {
	costs, err := c.calculate()
	if err != nil {
		log.Log.Error("failed to calculate pod costs")
		return err
	}

	for _, ci := range costs {
		for _, exp := range c.costExporters {
			exp.ExportCost(ci)
		}
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
		mux.Handle("/metrics", c.prometheusExporter)
		mux.Handle("/healthz", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close() // nolint: errcheck
				fmt.Fprintf(w, "ok") // nolint: errcheck
			},
		))

		s := http.Server{
			Addr:    c.listenAddr,
			Handler: mux,
		}
		log.Log.Infof("starting server on %s", c.listenAddr)

		go func() {
			<-ctx.Done()
			s.Shutdown(ctx) // nolint: gosec, errcheck
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
				if err := c.CalculateAndEmit(); err != nil {
					log.Log.Errorw("error during cost calculation cycle", zap.Error(err))
				}
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
