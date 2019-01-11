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
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/planetlabs/kostanza/internal/log"
)

var (
	// MeasurePubsubPublishErrors tracks publishing errors in the PubsubCostExporter.
	MeasurePubsubPublishErrors = stats.Int64("kostanza/measures/pubsub_errors", "Number of pubsub publish error", stats.UnitDimensionless)
)

// CostExporter emits CostItems - for example, as a metric or
// to a third-party system.
type CostExporter interface {
	ExportCost(cd CostData)
}

// StatsCostExporter emits metrics to a stats system.
type StatsCostExporter struct {
	mapper *Mapper
}

// NewStatsCostExporter returns a new StatsCostExporter.
func NewStatsCostExporter(mapper *Mapper) *StatsCostExporter {
	return &StatsCostExporter{
		mapper: mapper,
	}
}

// ExportCost emits cost data to the stats system.
func (sce *StatsCostExporter) ExportCost(cd CostData) {
	ctx, err := sce.mapTags(cd)
	if err != nil {
		log.Log.Errorw("could not update tag context from pod metadata", zap.Error(err))
	}
	stats.Record(ctx, MeasureCost.M(cd.Value))
}

func (sce *StatsCostExporter) mapTags(cd CostData) (context.Context, error) {
	ctx := context.Background()
	tags := []tag.Mutator{}
	for k, v := range cd.Dimensions {
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

// CostDataKey groups related cost data. Note: this isn't very space efficient
// at the moment given the duplication between it and the CostDataRow. We could,
// for example use a hashing function instead but this ought to be friendly
// for debugging in the short term.
type CostDataKey struct {
	// The kind of cost figure represented.
	Kind ResourceCostKind
	// The strategy the yielded this CostItem.
	Strategy string
	// Additional dimensions associated with the cost.
	Dimensions string
}

// MarshalLogObject exports CostData fields for the zap logger.
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

func (c *CostData) key() CostDataKey {
	dims := sort.StringSlice([]string{})
	for k, v := range c.Dimensions {
		dims = append(dims, fmt.Sprintf("%s:%s", k, v))
	}
	dims.Sort()
	return CostDataKey{
		Kind:       c.Kind,
		Strategy:   c.Strategy,
		Dimensions: strings.Join(dims, ","),
	}
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

// BufferingCostExporter is an exporter that locally merges similarly
// dimensioned data on the client before emitting to other exporters.
type BufferingCostExporter struct {
	ctx      context.Context
	buffer   map[CostDataKey]CostData
	interval time.Duration
	mux      sync.Mutex
	next     CostExporter
}

// NewBufferingCostExporter returns a BufferingCostExporter that flushes on the
// provided interval. The backgrounded flush procedure can be cancelled by
// cancelling the provided context. On every interval we emit aggregated cost
// metrics to the provided `next` CostExporter.
func NewBufferingCostExporter(ctx context.Context, interval time.Duration, next CostExporter) (*BufferingCostExporter, error) {
	bce := &BufferingCostExporter{
		ctx:      ctx,
		mux:      sync.Mutex{},
		buffer:   map[CostDataKey]CostData{},
		interval: interval,
		next:     next,
	}

	go func() {
		log.Log.Debug("starting background flush loop")
		bce.startFlusher()
		log.Log.Debug("background flush loop completed")
	}()

	return bce, nil
}

// ExportCost enqueues the CostData provided for subsequent emission to the next
// cost exporter. This serves to debounce repeated cost events and reduce load
// on the system.
func (bce *BufferingCostExporter) ExportCost(cd CostData) {
	bce.mux.Lock()
	defer bce.mux.Unlock()
	k := cd.key()
	v := bce.buffer[k].Value
	cd.Value += v
	bce.buffer[k] = cd
}

func (bce *BufferingCostExporter) startFlusher() {
	ticker := time.NewTicker(bce.interval)
	defer ticker.Stop()
	done := bce.ctx.Done()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			bce.flush()
		}
	}
}

func (bce *BufferingCostExporter) flush() {
	bce.mux.Lock()
	defer bce.mux.Unlock()
	log.Log.Debug("flushing buffered cost data")
	for _, v := range bce.buffer {
		bce.next.ExportCost(v)
	}
	bce.buffer = map[CostDataKey]CostData{}
}

// NewPubsubCostExporter creates a new PubsubCostExporter, instantiating an
// internal client against google cloud APIs.
func NewPubsubCostExporter(ctx context.Context, topic string, project string) (*PubsubCostExporter, error) {
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
		ctx:    ctx,
	}, nil
}

// ExportCost emits the CostItem to the PubsubCostExporter's configured pubsub topic.
func (pe *PubsubCostExporter) ExportCost(cd CostData) {
	msg, err := json.Marshal(cd)
	if err != nil {
		log.Log.Errorw("could not marshal cost", zap.Error(err))
		return
	}

	log.Log.Debugw("exporting cost data to pubsub", zap.Object("data", &cd))
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
