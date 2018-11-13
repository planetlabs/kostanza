# Kostanza

[![Docker Pulls](https://img.shields.io/docker/pulls/jacobstr/kostanza.svg)](https://hub.docker.com/r/jacobstr/kostanza/)
[![codecov](https://codecov.io/gh/jacobstr/kostanza/branch/master/graph/badge.svg)](https://codecov.io/gh/jacobstr/kostanza)
[![Build Status](https://travis-ci.com/jacobstr/kostanza.svg?branch=master)](https://travis-ci.com/jacobstr/kostanza)

Kostanza is a service for emitting approximate cost metrics for services
running in a kubernetes cluster. It works by:

- Periodically listing all pods.
- Determining what nodes those pods are running on.
- Using the pods cpu or memory utilization of the node to increment a cost counter.

Kostanza's pricing units are in millionths of a cent. The choice was made
since the intended sink for these metrics is a prometheus counter, which is
based on integers. Units of cents and thousandths of a cent were eschewed in
favor of increased granularity due to truncation. Kostanza may produce
inaccurate results for pods that use very little resources in conjunction
with rapid polling intervals.

# Configuration

## Pricing

Kostanza does not make assumptions about node costs, though this may be
obtained through cloud APIs in the future. Instead, it uses a user-provided
configuration file that allows admins to map node labels to appropriate
metadata regarding allocatable memory, cpu, and hourly instance cost. The
source-ordering of the `.Pricing.Entries` array in the examples below is
important: the first entry whose `Labels` collection is a subset of the
labels for a given node will be used. Thus, you should generally order your
table from specific to general.

> Note: we may use a simple heuristic of sorting entries by the number of
> labels you specify.

## Mapping

Kostanza does not make assumptions about the dimensions you want to use for
the cost metrics it emits. It allows you to map these from resource labels
and annotations. For example, organizations that use the label `service` may
wish to use the jsonPath expression `{.Pod.ObjectMeta.Labels.service}`.

It's important to set to a default value since prometheus, the canonical
(and to date, only) exporter will complain about the cardinality of metrics
when a key is left empty.

> Note: the property names are based on the CostItem struct contained
> within the package, which references kubernetes client-go, see
> https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/types.go

```json
{
  "Pricing": {
    "Entries": [
      {
        "Labels": {
          "beta.kubernetes.io/instance-type": "n1-standard-16"
        },
        "HourlyCostMicroCents": 400000000
      },
      {
        "Labels": {
          "kubernetes.io/hostname": "minikube"
        },
        "HourlyCostMicroCents": 10000000
      }
    ]
  },
  "Mapping": {
    "Entries": [
      {
        "Destination": "service",
        "Source": "{.Pod.ObjectMeta.Labels.service}",
        "Default": "unknown"
      },
      {
        "Destination": "component",
        "Source": "{.Pod.ObjectMeta.Labels.component}",
        "Default": "unknown"
      },
      {
        "Destination": "node_instance_type",
        "Source": "{.Node.ObjectMeta.Labels['beta.kubernetes.io/instance-type']}",
        "Default": "unknown"
      }
      {
        "Destination": "kind",
        "Source": "{.Kind}",
        "Default": "unknown"
      },
      {
        "Destination": "strategy",
        "Source": "{.Strategy}",
        "Default": "unknown"
      }
    ]
  }
}
```

## Strategies

Kostanza currently emits metrics according to two strategies by default:

- WeightedPricingStrategy
- NodePricingStrategy

### WeightedPricingStrategy

The `WeightedPricingStrategy` strategy operates as follows:

- First, add up all cpu and memory resource requests on all nodes.
- Next, for each pod, determine the fraction of memory and cpu it has
  requested on its respective node. The pod's cost is based on the average
  of these two fractions.

This strategy does punish pods for being scheduled to fresh nodes - the
choice is deliberate and is intended to capture costs incurred by services
that may trigger frequent scale ups without having a chance to benefit from
bin-packing additional pods.

### NodePricingStrategy

The `NodePricingStrategy` is intended to emit baseline cost metrics for your
nodes, i.e. to provide a total cost for your cluster. This can be useful in
visualizations where one wishes to graph pod costs as a fraction of total
cost. The mapping configuration presented earlier provides an example of how
one might configure your mapper based on nascent standardized labels (e.g.
`beta.kubernetes.io/instance-type`).

### Metric Dimensions

All strategies share the same metrics and metric dimensions. This means, for example,
that the `NodePricingStrategy` is likely to emit defaults for metrics only relevant
to pods. This is a deliberate design choice at the moment - rather than having multiple
measures or metrics, we have a single metric containing a superset of cost dimensions
whether they apply to a particular strategy or not.

# Exporters

Kostanza exports cost data in two ways: as prometheus metrics, and to
optionally to google pubsub. The pubsub export is largely intended to leave
the primary kostanza agent decoupled from any particular storage backend - however,
a secondary `aggregate` command is available as part of the kostanza binary to pipe
these pubsub message into an automatically-provisioned BigQuery table.

## Prometheus Exporter

Intended for real time data and convenient calculation of costs as rates.
Note that the cost counter will reset and it's absolute value should generally
not be relied on - you'll want to take use the PromQL `rate` function to express
costs as rates of change over time.

## Pubsub Exporter and the Aggregate Subcommand

For longer term analysis, kostanza allows for publishing messages to a pubsub
queue where they can be consumed and funneled into a data warehouse such as
GCP. Kostanza even offers a baked-in `aggregate` subcommand for the purpose.
The payload of these messages is default json encoding of the coster.CostData
structure:

```go
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
```

The `aggregate` subcommand will happily consume messages that adhere to this
general spefication and published to subscription specified by the
`pubsub-subscription` startup argument. This may be useful if you wish to
incorporate data from systems outside of kubernetes.

### Auto-provisioning

When the `aggregate` subcommand starts up, it will use the mapping defined in
your configuration file to automatically provision a BigQuery table in the
`bigquery-project` and `bigquery-dataset`, named according to the
`bigquery-table` CLI flag.

For convenience, we parse the [mapping](#mapping) specified in your
configuration and automatically attempt to create `Dimension_DestinationName`
columns for each of your mapping destinations. This can be incredibly
convenient for bootstrapping or local development but is not a full data
migration by any means: if the BigQuery table already exists, no attempt will
be infer schema or data migrations on your behalf. Generally, a best practice
may be to create an entirely new table if a new dimension is required.
