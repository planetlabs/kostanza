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
	"encoding/json"
	"fmt"
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

	"github.com/planetlabs/kostanza/internal/lister"
	"github.com/planetlabs/kostanza/internal/log"
)

// ResourceCostKind is used to indidicate what resource a cost was derived from.
type ResourceCostKind string

var (
	// ResourceCostCPU is a cost metric derived from CPU utilization.
	ResourceCostCPU = ResourceCostKind("cpu")
	// ResourceCostMemory is a cost metric derived from memory utilization.
	ResourceCostMemory = ResourceCostKind("memory")
	// ResourceCostGPU is a cost metric derived from GPU utilization. At the present
	// time kostanza assumes all GPU's in your cluster are homogenous.
	ResourceCostGPU = ResourceCostKind("gpu")
	// ResourceCostWeighted is a cost metric derived from a weighted average of memory and cpu utilization.
	ResourceCostWeighted = ResourceCostKind("weighted")
	// ResourceCostNode represents the overall cost of a node.
	ResourceCostNode = ResourceCostKind("node")
	// TagStatus indicates the success or failure of an operation.
	TagStatus, _       = tag.NewKey("status")
	tagStatusSucceeded = "succeeded"
	tagStatusFailed    = "failed"
)

var (
	// ErrNoPodNode may be returned during races where the pod containing a given
	// node has disappeared.
	ErrNoPodNode = errors.New("could not lookup node for pod")
	// ErrSenselessInterval is returned if the difference since our last run time
	// is less than 0. Obviously, this should never since time moves forward.
	ErrSenselessInterval = errors.New("senseless interval since last calculation")
)

var (
	// MeasureCost is the stat for tracking costs in millionths of a cent.
	MeasureCost = stats.Int64("kostanza/measures/cost", "Cost in millionths of a cent", "µ¢")
	// MeasureCycles is the number of tracking loops conducted.
	MeasureCycles = stats.Int64("kostanza/measures/cycles", "Iterations executed", stats.UnitDimensionless)
	// MeasureLag is the discrepancy between the ideal interval and actual interval between calculations.
	MeasureLag = stats.Float64("kostanza/measures/lag", "Lag time in calculation intervals", stats.UnitMilliseconds)
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
		strategies:         []PricingStrategy{GPUPricingStrategy, CPUPricingStrategy, MemoryPricingStrategy, WeightedPricingStrategy, NodePricingStrategy},
		podFilters:         PodFilters{RunningPodFilter},
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
	podFilters         PodFilters
	lastRun            time.Time
}

func (c *coster) filterPod(p *core_v1.Pod) bool {
	for _, f := range c.podFilters {
		if f(p) {
			return true
		}
	}
	return false
}

func (c *coster) applyPodFilters(pods []*core_v1.Pod) []*core_v1.Pod {
	ret := []*core_v1.Pod{}
	for _, p := range pods {
		if !c.podFilters.All(p) {
			continue
		}
		ret = append(ret, p)
	}
	return ret
}

// Calculate returns a slice of podCostItem records that expose
// pricing details for services.
func (c *coster) calculate() ([]CostItem, error) {
	log.Log.Debug("cost calculation loop triggered")

	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	pods = c.applyPodFilters(pods)

	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	cis := []CostItem{}

	// Fairly unimpressive cruft to measure lag between our desired interval and
	// actual interval since the last calculate() call. If this is signficant you
	// may want to feed the program more cpu.
	var interval time.Duration
	if c.lastRun.IsZero() {
		interval = c.interval
		c.lastRun = time.Now()
	} else {
		t := time.Now()
		interval = t.Sub(c.lastRun)
		if interval <= 0 {
			return nil, ErrSenselessInterval
		}

		c.lastRun = t
		lag := float64((interval / time.Millisecond) - (c.interval / time.Millisecond))
		stats.Record(context.Background(), MeasureLag.M(lag))
	}

	for _, s := range c.strategies {
		cis = append(cis, s.Calculate(c.config.Pricing, interval, pods, nodes)...)
	}
	return cis, nil
}

func (c *coster) CalculateAndEmit() error {
	costs, err := c.calculate()
	if err != nil {
		log.Log.Error("failed to calculate pod costs")
		ctx, _ := tag.New(context.Background(), tag.Upsert(TagStatus, tagStatusFailed)) // nolint: gosec
		stats.Record(ctx, MeasureCycles.M(1))
		return err
	}

	mapper := &c.config.Mapper
	for _, ci := range costs {
		for _, exp := range c.costExporters {
			dims, err := mapper.MapData(ci)
			if err != nil {
				log.Log.Error("could not map data", zap.Error(err))
				continue
			}
			ce := CostData{
				Kind:       ci.Kind,
				Strategy:   ci.Strategy,
				Value:      ci.Value,
				Dimensions: dims,
				EndTime:    time.Now(),
			}
			exp.ExportCost(ce)
		}
	}

	ctx, _ := tag.New(context.Background(), tag.Upsert(TagStatus, tagStatusSucceeded)) // nolint: gosec
	stats.Record(ctx, MeasureCycles.M(1))

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
