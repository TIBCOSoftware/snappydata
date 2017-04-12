# enable-network-partition-detection

## Description

Boolean instructing the system to detect and handle splits in the distributed system, typically caused by a partitioning of the network (split brain) where the distributed system is running. See <a href="../../manage_guide/Topics/network_partition.md#concept_E4FF4D3C8FBE47A38BD5E2A6B5B14852" class="xref" title="When network segmentation occurs, a distributed system that does not handle the partition condition properly allows multiple subgroups to form. This condition can lead to numerous problems, including distributed applications operating on inconsistent data.">Detecting and Handling Network Segmentation (&quot;Split Brain&quot;)</a>.

## Default Value

false

## Property Type

connection (boot)

## Prefix

gemfire.
