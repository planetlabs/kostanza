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

	"github.com/jacobstr/kostanza/internal/aggregator"
	"github.com/jacobstr/kostanza/internal/coster"
	"github.com/jacobstr/kostanza/internal/kubernetes"
	"github.com/jacobstr/kostanza/internal/log"
)

const name = "kostanza"

var (
	app       = kingpin.New("kostanza", "A Kubernetes component to emit cost metrics for services.")
	verbosity = app.Flag("verbosity", "Logging verbosity level.").Short('v').Counter()
	config    = app.Flag("config", "Path to configuration json.").Required().File()

	collect                    = app.Command("collect", "Starts up kostanza in collection mode.")
	listenAddr                 = collect.Flag("listen-addr", "Listen address for prometheus metrics and health checks.").Default(":5000").String()
	kubecfg                    = collect.Flag("kubeconfig", "Path to kubeconfig file. Leave unset to use in-cluster config.").String()
	apiserver                  = collect.Flag("master", "Address of Kubernetes API server. Leave unset to use in-cluster config.").String()
	interval                   = collect.Flag("interval", "Cost calculation interval.").Default("10s").Duration()
	collectPubsubFlushInterval = collect.Flag("pubsub-flush-interval", "Pubsub buffer flush interval").Default("300s").Duration()
	collectPubsubTopic         = collect.Flag("pubsub-topic", "Pubsub topic name for publishing cost metrics.").String()
	collectPubsubProject       = collect.Flag("pubsub-project", "Pubsub project name for publishing cost metrics.").String()

	aggregate                   = app.Command("aggregate", "Starts up kostanza in pubsub aggregation mode.")
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
		Description: "Total pubsub publish errors",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{},
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

		c, err := kubernetes.BuildConfigFromFlags(*apiserver, *kubecfg)
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
		kingpin.FatalIfError(view.Register(viewCosts, viewPubsubErrors), "cannot register metrics")
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

			ce, err := coster.NewPubsubCostExporter(ctx, *collectPubsubTopic, *collectPubsubProject)
			kingpin.FatalIfError(err, "could not create pubsub cost exporter")

			bce, err := coster.NewBufferingCostExporter(ctx, *collectPubsubFlushInterval, ce)
			kingpin.FatalIfError(err, "could not create buffering cost exporter")

			ces = append(ces, bce)
		}

		coster, err := coster.NewKubernetesCoster(*interval, cf, cs, p, *listenAddr, ces)
		kingpin.FatalIfError(err, "cannot create coster")

		kingpin.FatalIfError(coster.Run(ctx), "exited with error")
	case aggregate.FullCommand():
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cf, err := coster.NewConfigFromReader(*config)
		kingpin.FatalIfError(err, "cannot read configuration data")

		agg, err := aggregator.NewBigQueryAggregator(
			ctx,
			*aggregatePubsubProject,
			*aggregateBigQueryDataset,
			*aggregateBigQueryTable,
			&cf.Mapper,
		)
		kingpin.FatalIfError(err, "could not create aggregator")

		con, err := aggregator.NewPubsubConsumer(
			ctx,
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
