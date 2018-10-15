package coster

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/pubsub"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/jacobstr/kostanza/internal/log"
)

var (
	// MeasurePubsubPublishErrors tracks publishing errors in the PubsubCostExporter.
	MeasurePubsubPublishErrors = stats.Int64("kostanza/measures/pubsub_errors", "Number of pubsub publish error", stats.UnitDimensionless)
)

// CostExporter emits CostItems - for example, as a metric or
// to a third-party system.
type CostExporter interface {
	ExportCost(ci CostItem)
}

// StatsCostExporter emits metrics to a stats system.
type StatsCostExporter struct {
	mapper *Mapper
}

// NewStatsExporter returns a new StatsCostExporter.
func NewStatsCostExporter(mapper *Mapper) *StatsCostExporter {
	return &StatsCostExporter{
		mapper: mapper,
	}
}

// ExportCost emits cost data to the stats system.
func (sce *StatsCostExporter) ExportCost(ci CostItem) {
	ctx, err := sce.mapTags(ci)
	if err != nil {
		log.Log.Errorw("could not update tag context from pod metadata", zap.Error(err))
	}
	stats.Record(ctx, MeasureCost.M(ci.Value))
}

func (sce *StatsCostExporter) mapTags(ci CostItem) (context.Context, error) {
	ctx := context.Background()

	d, err := sce.mapper.MapData(ci)
	if err != nil {
		return nil, err
	}

	tags := []tag.Mutator{}
	for k, v := range d {
		t, err := tag.NewKey(k)
		if err != nil {
			return nil, err
		}

		tags = append(tags, tag.Upsert(t, v))
	}

	return tag.New(ctx, tags...)
}

// PubsubCostExporter emits data to pubsub.
type PubsubCostExporter struct {
	mapper *Mapper
	client *pubsub.Client
	topic  *pubsub.Topic
	ctx    context.Context
}

// CostData models pubsub-exported cost metadata.
type CostData struct {
	// The kind of cost figure represented.
	Kind ResourceCostKind
	// The strategy the yielded this CostItem.
	Strategy string
	// The value in microcents that it costs.
	Value int64
	// Additional dimensions associated with the cost.
	Dimensions map[string]string
	// The interval for which this metric was created.
	EndTime time.Time
}

// MarhshalLogObject exports CostData fields for the zap logger.
func (c *CostData) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("Kind", string(c.Kind))
	enc.AddString("Strategy", c.Strategy)
	enc.AddTime("EndTime", c.EndTime)
	enc.AddInt64("Value", c.Value)
	for k, v := range c.Dimensions {
		enc.AddString("Dimensions."+k, v)
	}
	return nil
}

func createTopicIfNotExists(ctx context.Context, client *pubsub.Client, topic string) (*pubsub.Topic, error) {
	t := client.Topic(topic)

	ok, err := t.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if ok {
		return t, nil
	}

	log.Log.Infow(
		"pubsub topic did not exist, created it",
		zap.String("topic", topic),
	)
	nt, err := client.CreateTopic(ctx, topic)
	if err != nil {
		return nil, err
	}

	return nt, nil
}

// NewPubsubCostExporter creates a new PubsubCostExporter, instantiating an
// internal client against google cloud APIs.
func NewPubsubCostExporter(ctx context.Context, topic string, project string, mapper *Mapper) (*PubsubCostExporter, error) {
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	t, err := createTopicIfNotExists(ctx, client, topic)
	if err != nil {
		return nil, err
	}

	return &PubsubCostExporter{
		client: client,
		topic:  t,
		mapper: mapper,
		ctx:    ctx,
	}, nil
}

// ExportCost emits the CostItem to the PubsubCostExporter's configured pubsub topic.
func (pe *PubsubCostExporter) ExportCost(ci CostItem) {
	dims, err := pe.mapper.MapData(ci)
	if err != nil {
		log.Log.Errorw("could not export cost", zap.Error(err))
		return
	}

	ce := CostData{
		Kind:       ci.Kind,
		Strategy:   ci.Strategy,
		Value:      ci.Value,
		Dimensions: dims,
		EndTime:    time.Now(),
	}

	msg, err := json.Marshal(ce)
	if err != nil {
		log.Log.Errorw("could not marshal cost", zap.Error(err))
		return
	}

	log.Log.Debugw("exporting cost data to pubsub", zap.Object("data", &ce))
	res := pe.topic.Publish(pe.ctx, &pubsub.Message{Data: msg})
	go func(res *pubsub.PublishResult) {
		_, err := res.Get(pe.ctx)
		if err != nil {
			log.Log.Errorw("Failed to publish", zap.Error(err))
			stats.Record(pe.ctx, MeasurePubsubPublishErrors.M(1))
			return
		}
	}(res)
}
