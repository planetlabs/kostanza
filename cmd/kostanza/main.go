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

	"github.com/jacobstr/kostanza/internal/coster"
	"github.com/jacobstr/kostanza/internal/kubernetes"
	"github.com/jacobstr/kostanza/internal/log"
)

const Name = "kostanza"

var (
	app        = kingpin.New("kostanza", "A Kubernetes component to emit cost metrics for services.")
	listenAddr = app.Flag("listen-addr", "Listen address for prometheus metrics and health checks.").Default(":5000").String()
	verbosity  = app.Flag("verbosity", "Logging verbosity level.").Short('v').Counter()
	kubecfg    = app.Flag("kubeconfig", "Path to kubeconfig file. Leave unset to use in-cluster config.").String()
	apiserver  = app.Flag("master", "Address of Kubernetes API server. Leave unset to use in-cluster config.").String()
	interval   = app.Flag("interval", "Cost calculation interval.").Default("10s").Duration()
	config     = app.Flag("config", "Path to configuration json.").Required().File()

	serve = app.Command("serve", "Run the kostanza daemon.")
)

var (
	viewCosts = &view.View{
		Name:        "costs",
		Measure:     coster.MeasureCost,
		Description: "Cost of services in millionths of a cent.",
		Aggregation: view.Sum(),
		TagKeys:     []tag.Key{coster.TagKind},
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
	case serve.FullCommand():
		c, err := kubernetes.BuildConfigFromFlags(*apiserver, *kubecfg)
		kingpin.FatalIfError(err, "cannot create Kubernetes client configuration")

		cs, err := client.NewForConfig(c)
		kingpin.FatalIfError(err, "cannot create Kubernetes client")

		cf, err := coster.NewConfigFromReader(*config)
		kingpin.FatalIfError(err, "cannot read configuration data")

		p, err := prometheus.NewExporter(prometheus.Options{Namespace: Name})
		kingpin.FatalIfError(err, "cannot export metrics")

		mk, err := cf.Mapper.TagKeys()
		kingpin.FatalIfError(err, "could not prepare metric tags from mapping")

		viewCosts.TagKeys = append(viewCosts.TagKeys, mk...)
		kingpin.FatalIfError(view.Register(viewCosts), "cannot register metrics")
		view.RegisterExporter(p)

		coster, err := coster.NewKubernetesCoster(*interval, cf, cs, p, *listenAddr)
		kingpin.FatalIfError(err, "cannot create coster")
		coster.Run(context.Background())
	}
}

// Many Kubernetes client things depend on glog. glog gets sad when flag.Parse()
// is not called before it tries to emit a log line. flag.Parse() fights with
// kingpin.
func glogWorkaround() {
	os.Args = []string{os.Args[0], "-logtostderr=true", "-v=5", "-alsologtostderr"}
	flag.Parse()
}
