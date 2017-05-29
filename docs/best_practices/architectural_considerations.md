## Overview
This section presents advice on the optimal way to use SnappyData. It also gives you practical advice to assist you in configuring SnappyData and efficiently analysing your data.

## Architectural Considerations

To determine the configuration that is best suited for your environment some experimentation and testing is required involving representative data and workload.

Before we start, we have to make some assumptions:

* Data will be in memory: <mark>How much in memory.</mark>

* Concurrency:  <mark>Short running queries or long running queries.</mark>

* Right Affinity Mode to Use