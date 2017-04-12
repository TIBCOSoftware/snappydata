# default-recovery-delay

## Description

Specifies a default `RECOVERYDELAY` value for all tables in the cluster. If a RowStore member leaves the cluster and no new members join, the remaining RowStore members wait for a period of time before they perform recovery to satisfy the redundancy requirements of partitioned tables. This attribute specifies the period of time to wait before initiating recovery. 

!!!Note 
	RowStore always initiates recovery for redundant, partitioned tables when a new RowStore member joins the cluster or when a rebalance operation occurs.</p>

A value of -1, the default value, indicates that no recovery is performed unless a new RowStore member joins the cluster, or unless individual tables override the recovery delay by specifying the `RECOVERYDELAY` clause in their `CREATE TABLE` statements. See <a href="../../data_management/overview_how_pr_ha_works.md#how_pr_ha_works" class="xref" title="By default, RowStore stores only a single copy of your partitioned table data among the table&#39;s data stores. You can configure RowStore to maintain redundant copies of your partitioned table data for high availability.">Configuring High Availability for Partitioned Tables</a>.

## Default Value

-1

## Property Type

connection (boot)

## Prefix

snappydata.
