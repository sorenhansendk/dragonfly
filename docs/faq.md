# Dragonfly Frequently Asked Questions

- [Dragonfly Frequently Asked Questions](#dragonfly-frequently-asked-questions)
  - [What is the license model of Dragonfly? Is it an open source?](#what-is-the-license-model-of-dragonfly-is-it-an-open-source)
  - [Can I use dragonfly in production?](#can-i-use-dragonfly-in-production)
  - [We benchmarked Dragonfly and we have not reached 4M qps throughput as you advertised.](#we-benchmarked-dragonfly-and-we-have-not-reached-4m-qps-throughput-as-you-advertised)
  - [Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.](#dragonfly-provides-vertical-scale-but-we-can-achieve-similar-throughput-with-x-nodes-in-a-redis-cluster)
  - [If only Dragonfly had this command I would use it for sure](#if-only-dragonfly-had-this-command-i-would-use-it-for-sure)


## What is the license model of Dragonfly? Is it an open source?
Dragonfly is released under [BSL 1.1](../LICENSE.md) (Business Source License).
BSL is considered to be "source available" license and it's not strictly open-source license.
We believe that a BSL license is more permissive than licenses like AGPL, and it will allow us to
provide a competitive commercial service using our technology. In general terms,
it means that Dragonfly's code is free to use and free to change as long as you do not sell services directly related to
Dragonfly or in-memory datastores.
We followed the trend of other technological companies like Elastic, Redis, MongoDB, Cockroach labs, Redpanda Data to protect our rights to provide service and support for the software we are building.

## Can I use dragonfly in production?
License wise you are free to use dragonfly in your production as long as you do not provide Dragonfly as a managed service. From a code maturity point of view, Dragonfly's code is covered with unit testing. However as with any new software there are use cases that are hard to test and predict. We advise you to run your own particular use case on dragonfly for a few days before considering production usage.

## We benchmarked Dragonfly and we have not reached 4M qps throughput as you advertised.
We conducted our experiments using a load-test generator called `memtier_benchmark`,
and we run benchmarks on AWS network-enhanced instance `c6gn.16xlarge` on recent Linux kernel versions.
Dragonfly might reach smaller throughput on other instances, but we would
still expect to reach around 1M+ qps on instances with 16-32 vCPUs.

## Dragonfly provides vertical scale, but we can achieve similar throughput with X nodes in a Redis cluster.
Dragonfly utilizes the underlying hardware in an optimal way. Meaning it can run on small 8GB instances and scale verticly to large 768GB machines with 64 cores. This versatility allows to drastically reduce complexity of running cluster workloads to a single node saving hardware resources and costs. More importantly, it reduces the complexity (total cost of ownership) of handling the multi-node cluster. In addition, Redis cluster-mode imposes some limitations on multi-key and transactinal operations while Dragonfly provides the same semantics as single node Redis.
Finally, scaling out horizontally with small instances can actually cause instability issues in your production
environment. Large scale deployments of in-memory stores need to scale both vertically and horizontally and you
can not do it efficiently with in-memory store like Redis.

## If only Dragonfly had this command I would use it for sure
Dragonfly implements ~190 Redis commands which we think represent a good coverage of the market. However this is not based empirical data. Having said that, if you have commands that are not covered, please feel free to open an issue for that or vote for an existing issue. We will do our best to prioritise those commands according to their popularity.
