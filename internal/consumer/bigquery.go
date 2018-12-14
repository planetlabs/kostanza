package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"go.opencensus.io/exporter/prometheus"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"

	"github.com/jacobstr/kostanza/internal/coster"
	"github.com/jacobstr/kostanza/internal/log"
)

var (
	// MeasureConsume measures aggregation results into the pubsub queue.
	MeasureConsume = stats.Int64("kostanza_aggregator/measures/consume", "Consumption operations", stats.UnitDimensionless)
	// TagConsumeStatus indicates the success or failure of a consumption
	TagConsumeStatus, _ = tag.NewKey("status")

	tagStatusSucceeded = "succeeded"
	tagStatusFailed    = "failed"
)

func isAlreadyExistsError(err error) bool {
	if gerr, ok := err.(*googleapi.Error); ok {
		if gerr.Code == 409 {
			return true
		}
	}
	return false
}

func isNotFoundError(err error) bool {
	if gerr, ok := err.(*googleapi.Error); ok {
		if gerr.Code == 404 {
			return true
		}
	}
	return false
}

// CostRow augments CostData with BigQuery specific interfaces for import
// purposes via the bigquery.Uploader.
type CostRow struct {
	coster.CostData
}

// Save prepares a CostRow for import into BigQuery.
func (ce CostRow) Save() (row map[string]bigquery.Value, insertID string, err error) {
	dims, err := json.Marshal(ce.CostData.Dimensions)
	if err != nil {
		return nil, "", err
	}

	e := map[string]bigquery.Value{
		"Kind":       string(ce.CostData.Kind),
		"Strategy":   ce.CostData.Strategy,
		"Value":      ce.CostData.Value,
		"EndTime":    ce.CostData.EndTime,
		"Dimensions": string(dims),
	}

	for k, v := range ce.CostData.Dimensions {
		e["Dimensions_"+k] = v
	}

	log.Log.Debugf("insertion data: %#v", e)

	return e, "", nil
}

func defaultSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "Kind", Type: bigquery.StringFieldType},
		{Name: "Strategy", Type: bigquery.StringFieldType},
		{Name: "Value", Type: bigquery.IntegerFieldType},
		{Name: "EndTime", Type: bigquery.TimestampFieldType},
		{Name: "Dimensions", Type: bigquery.StringFieldType},
	}
}

// MapperToSchema creates a BigQuery schema representation for the provided
// coster.Mapper configuration.
func MapperToSchema(mapper *coster.Mapper) bigquery.Schema {
	// For a quality example of creating a schema by hand see:
	// https://cloud.google.com/bigquery/docs/nested-repeatedThe
	s := defaultSchema()

	for _, m := range mapper.Entries {
		f := &bigquery.FieldSchema{
			Name: "Dimensions_" + m.Destination,
			Type: bigquery.StringFieldType,
		}
		s = append(s, f)
	}

	return s
}

// Consumer consumes messages as long as the provided context is not canceled,
// or its deadline exceeded.
type Consumer interface {
	Consume(ctx context.Context) error
}

// PubsubConsumer consumers messages from pubsub and forwards them to the
// provided aggregator.
type PubsubConsumer struct {
	subscription       *pubsub.Subscription
	aggregator         Aggregator
	listenAddr         string
	prometheusExporter *prometheus.Exporter
}

// NewPubsubConsumer consumes messages from pubsub and invokes the provider
// aggregator with the message contents.
func NewPubsubConsumer(ctx context.Context, prometheusExporter *prometheus.Exporter, listenAddr string, project string, topic string, subscription string, aggregator Aggregator) (*PubsubConsumer, error) {
	psClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		log.Log.Errorw("could not create pubsub client", zap.Error(err))
		return nil, err
	}

	sub, err := createSubscriptionIfNotExists(ctx, psClient, subscription, topic)
	if err != nil {
		return nil, err
	}

	return &PubsubConsumer{
		subscription:       sub,
		listenAddr:         listenAddr,
		aggregator:         aggregator,
		prometheusExporter: prometheusExporter,
	}, nil
}

// Consume begins the message consumption loop. It also registers and serves the
// `/metrics` and `/healthz` endpoints for monitoring purposes.
func (pc *PubsubConsumer) Consume(ctx context.Context) error {
	ctx, done := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer done()

		mux := http.NewServeMux()
		mux.Handle("/metrics", pc.prometheusExporter)
		mux.Handle("/healthz", http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close() // nolint: errcheck
				fmt.Fprintf(w, "ok") // nolint: errcheck
			},
		))

		s := http.Server{
			Addr:    pc.listenAddr,
			Handler: mux,
		}
		log.Log.Infof("starting server on %s", pc.listenAddr)

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

		return pc.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			var ce coster.CostData
			if err := json.Unmarshal(msg.Data, &ce); err != nil {
				log.Log.Errorw("could not decode message data", zap.Error(err), zap.ByteString("data", msg.Data))
				msg.Ack()

				ctx, _ = tag.New(ctx, tag.Upsert(TagConsumeStatus, tagStatusFailed)) // nolint: gosec
				stats.Record(ctx, MeasureConsume.M(1))
				return
			}

			if err := pc.aggregator.Aggregate(ctx, ce); err != nil {
				log.Log.Errorw("could not aggregate cost data", zap.Error(err))
				msg.Ack()

				ctx, _ = tag.New(ctx, tag.Upsert(TagConsumeStatus, tagStatusFailed)) // nolint: gosec
				stats.Record(ctx, MeasureConsume.M(1))
				return
			}

			msg.Ack()
			ctx, _ = tag.New(ctx, tag.Upsert(TagConsumeStatus, tagStatusSucceeded)) // nolint: gosec
			stats.Record(ctx, MeasureConsume.M(1))
			return
		})
	})

	return g.Wait()
}

// Aggregator coalesces and persists coster.CostData from kostanza.
type Aggregator interface {
	Aggregate(ctx context.Context, ce coster.CostData) error
}

// BigQueryAggregator coalesces and persists coster.CosData data to BigQuery.
type BigQueryAggregator struct {
	table    *bigquery.Table
	uploader *bigquery.Uploader
}

// NewBigQueryAggregator creates a new Aggregator that publishes consumed pubsub
// events to the named BigQuery dataset and table. It will attempt to provision
// the table using a schema inferred from the current version of the
// application if the table does not yet exist.
func NewBigQueryAggregator(ctx context.Context, project string, dataset string, table string, mapper *coster.Mapper) (*BigQueryAggregator, error) {
	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Log.Errorw("could not create bigquery client", zap.Error(err))
		return nil, err
	}

	ds := bqClient.Dataset(dataset)
	if err := ds.Create(ctx, nil); err != nil && !isAlreadyExistsError(err) {
		log.Log.Errorw("could not create dataset", zap.Error(err))
		return nil, err
	}

	tbl := ds.Table(table)
	if err := createTableIfNotExists(ctx, tbl, mapper); err != nil {
		return nil, err
	}

	return &BigQueryAggregator{
		table:    tbl,
		uploader: tbl.Uploader(),
	}, nil
}

func createSubscriptionIfNotExists(ctx context.Context, client *pubsub.Client, subscriptionName string, topicName string) (*pubsub.Subscription, error) {
	sub := client.Subscription(subscriptionName)

	if exists, err := sub.Exists(ctx); err != nil {
		log.Log.Errorw("could not create pubsub client", zap.Error(err))
		return nil, err
	} else if exists {
		return sub, nil
	}

	if _, err := client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: client.Topic(topicName)}); err != nil {
		log.Log.Errorw("could not create subscription", zap.Error(err))
		return nil, err
	}

	return sub, nil
}

func createTableIfNotExists(ctx context.Context, table *bigquery.Table, mapper *coster.Mapper) error {
	meta, err := table.Metadata(ctx)
	if err == nil {
		log.Log.Debugw("got metadata for table", zap.String("id", meta.FullID))
		return nil
	} else if err != nil && !isNotFoundError(err) {
		log.Log.Errorw("could not get metadata", zap.Error(err))
		return err
	}

	schema := MapperToSchema(mapper)
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		log.Log.Errorw("could not create table", zap.Error(err))
		return err
	}

	return nil
}

// Aggregate pushes coster.CostData to BigQuery.
func (ba *BigQueryAggregator) Aggregate(ctx context.Context, ce coster.CostData) error {
	cr := CostRow{ce}
	log.Log.Debugw("aggregating object", zap.Object("CostData", &ce))
	if err := ba.uploader.Put(ctx, cr); err != nil {
		log.Log.Errorw("could not insert row", zap.Error(err))
		if pmErr, ok := err.(bigquery.PutMultiError); ok {
			for _, rowInsertionError := range pmErr {
				log.Log.Debugw("row insertion error", zap.Error(&rowInsertionError))
			}
		}
		return err
	}
	return nil
}
