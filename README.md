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
wish to use the jsonPath expression `{.ObjectMeta.Labels.service}`.

It's important to set to a default value since prometheus, the canonical
(and to date, only) exporter will complain about the cardinality of metrics
when a key is left empty.

> Note: the property names are based on kubernetes client-go, see
> https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/types.go

```json
{
  "Pricing": {
    "Entries": [
      {
        "Labels": {
          "beta.kubernetes.io/instance-type": "n1-standard-16"
        },
        "TotalMilliCPU": 15750,
        "TotalMemoryBytes": 62950191104,
        "HourlyCostMicroCents": 400000000
      },
      {
        "Labels": {
          "kubernetes.io/hostname": "minikube"
        },
        "TotalMilliCPU": 2000,
        "TotalMemoryBytes": 1982693376,
        "HourlyCostMicroCents": 10000000
      }
    ]
  },
  "Mapping": {
    "Entries": [
      {
        "Destination": "service",
        "Source": "{.ObjectMeta.Labels.service}",
        "Default": "unknown"
      },
      {
        "Destination": "component",
        "Source": "{.ObjectMeta.Labels.component}",
        "Default": "unknown"
      }
    ]
  }
}
```
