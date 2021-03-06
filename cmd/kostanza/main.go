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

package main

import (
	"context"
	"flag"
	"os"

	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
	client "k8s.io/client-go/kubernetes"

	"github.com/planetlabs/kostanza/internal/consumer"
	"github.com/planetlabs/kostanza/internal/coster"
	"github.com/planetlabs/kostanza/internal/kubernetes"
	"github.com/planetlabs/kostanza/internal/log"
)

const name = "kostanza"

var (
	app       = kingpin.New("kostanza", "A Kubernetes component to emit cost metrics for services.")
	verbosity = app.Flag("verbosity", "Logging verbosity level.").Short('v').Counter()
	config    = app.Flag("config", "Path to configuration json.").Required().File()

	collect                    = app.Command("collect", "Starts up kostanza in cost data collection mode.")
	collectListenAddr          = collect.Flag("listen-addr", "Listen address for prometheus metrics and health checks.").Default(":5000").String()
	collectKubecfg             = collect.Flag("kubeconfig", "Path to kubeconfig file. Leave unset to use in-cluster config.").String()
	collectApiserver           = collect.Flag("master", "Address of Kubernetes API server. Leave unset to use in-cluster config.").String()
	collectInterval            = collect.Flag("interval", "Cost calculation interval.").Default("10s").Duration()
	collectPubsubFlushInterval = collect.Flag("pubsub-flush-interval", "Pubsub buffer flush interval").Default("300s").Duration()
	collectPubsubTopic         = collect.Flag("pubsub-topic", "Pubsub topic name for publishing cost metrics.").String()
	collectPubsubProject       = collect.Flag("pubsub-project", "Pubsub project name for publishing cost metrics.").String()

	aggregate                   = app.Command("aggregate", "Starts up kostanza in pubsub consumption mode.")
	aggregateListenAddr         = aggregate.Flag("listen-addr", "Listen address for prometheus metrics and health checks.").Default(":5000").String()
	aggregatePubsubTopic        = aggregate.Flag("pubsub-topic", "Pubsub topic name for binding the cost subscription automatically.").Required().String()
	aggregatePubsubSubscription = aggregate.Flag("pubsub-subscription", "Pubsub subscription name for pulling cost metrics.").Required().String()
	aggregatePubsubProject      = aggregate.Flag("pubsub-project", "Pubsub project name for publishing cost metrics.").Required().String()
	aggregateBigQueryProject    = aggregate.Flag("bigquery-project", "Project containing the BigQuery database for collecting cost metrics.").Required().String()
	aggregateBigQueryDataset    = aggregate.Flag("bigquery-dataset", "Name of the BigQuery dataset to push cost data into.").Required().String()
	aggregateBigQueryTable      = aggregate.Flag("bigquery-table", "Name of the BigQuery table within the specified dataset to push cost data into.").Required().String()
)

var (
	viewCosts = &view.View{
		Name:        "costs",
		Measure:     coster.MeasureCost,
		Description: "Cost of services in millionths of a cent.",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{},
	}

	viewPubsubErrors = &view.View{
		Name:        "pubsub_errors_total",
		Measure:     coster.MeasurePubsubPublishErrors,
		Description: "Total pubsub publish errors.",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{},
	}

	viewCycles = &view.View{
		Name:        "cycles",
		Measure:     coster.MeasureCycles,
		Description: "Total observation cycles.",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{},
	}

	viewLag = &view.View{
		Name:        "lag",
		Measure:     coster.MeasureLag,
		Description: "Lag time of cost calculation loops.",
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{},
	}

	viewConsume = &view.View{
		Name:        "consume_consumed_total",
		Measure:     consumer.MeasureConsume,
		Description: "Total aggregator consumption operations.",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{consumer.TagConsumeStatus},
	}
)

func main() {
	parsed := kingpin.MustParse(app.Parse(os.Args[1:]))
	glogWorkaround()

	if *verbosity > 0 {
		log.Cfg.Level.SetLevel(zap.DebugLevel)
		log.Log.Debug("using increased logging verbosity")
	}

	switch parsed {
	case collect.FullCommand():
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c, err := kubernetes.BuildConfigFromFlags(*collectApiserver, *collectKubecfg)
		kingpin.FatalIfError(err, "cannot create Kubernetes client configuration")

		cs, err := client.NewForConfig(c)
		kingpin.FatalIfError(err, "cannot create Kubernetes client")

		cf, err := coster.NewConfigFromReader(*config)
		kingpin.FatalIfError(err, "cannot read configuration data")

		p, err := prometheus.NewExporter(prometheus.Options{Namespace: name})
		kingpin.FatalIfError(err, "cannot export metrics")

		mk, err := cf.Mapper.TagKeys()
		kingpin.FatalIfError(err, "could not prepare metric tags from mapping")

		viewCosts.TagKeys = append(viewCosts.TagKeys, mk...)
		kingpin.FatalIfError(view.Register(viewCosts, viewPubsubErrors, viewCycles, viewLag), "cannot register metrics")
		view.RegisterExporter(p)

		ces := []coster.CostExporter{
			coster.NewStatsCostExporter(&cf.Mapper),
		}

		if *collectPubsubTopic != "" {
			log.Log.Infow(
				"pubsub exporter enabled",
				zap.String("topic", *collectPubsubTopic),
				zap.String("project", *collectPubsubProject),
			)

			ce, err := coster.NewPubsubCostExporter(ctx, *collectPubsubTopic, *collectPubsubProject) // nolint: vetshadow
			kingpin.FatalIfError(err, "could not create pubsub cost exporter")

			bce, err := coster.NewBufferingCostExporter(ctx, *collectPubsubFlushInterval, ce)
			kingpin.FatalIfError(err, "could not create buffering cost exporter")

			ces = append(ces, bce)
		}

		coster, err := coster.NewKubernetesCoster(*collectInterval, cf, cs, p, *collectListenAddr, ces)
		kingpin.FatalIfError(err, "cannot create coster")

		kingpin.FatalIfError(coster.Run(ctx), "exited with error")
	case aggregate.FullCommand():
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cf, err := coster.NewConfigFromReader(*config)
		kingpin.FatalIfError(err, "cannot read configuration data")

		p, err := prometheus.NewExporter(prometheus.Options{Namespace: name})
		kingpin.FatalIfError(err, "cannot export metrics")

		kingpin.FatalIfError(view.Register(viewConsume), "cannot register metrics")
		view.RegisterExporter(p)

		agg, err := consumer.NewBigQueryAggregator(
			ctx,
			*aggregatePubsubProject,
			*aggregateBigQueryDataset,
			*aggregateBigQueryTable,
			&cf.Mapper,
		)
		kingpin.FatalIfError(err, "could not create aggregator")

		con, err := consumer.NewPubsubConsumer(
			ctx,
			p,
			*aggregateListenAddr,
			*aggregateBigQueryProject,
			*aggregatePubsubTopic,
			*aggregatePubsubSubscription,
			agg,
		)
		kingpin.FatalIfError(err, "could not create pubsub consumer")

		kingpin.FatalIfError(con.Consume(ctx), "failed consumption loop")
	}
}

// Many Kubernetes client things depend on glog. glog gets sad when flag.Parse()
// is not called before it tries to emit a log line. flag.Parse() fights with
// kingpin.
func glogWorkaround() {
	os.Args = []string{os.Args[0], "-logtostderr=true", "-v=5", "-alsologtostderr"}
	flag.Parse()
}
