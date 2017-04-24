# default-recovery-delay

## Description

Specifies a default `RECOVERYDELAY` value for all tables in the cluster. If a SnappyData member leaves the cluster and no new members join, the remaining SnappyData members wait for a period of time before they perform recovery to satisfy the redundancy requirements of partitioned tables. This attribute specifies the period of time to wait before initiating recovery. 

!!!Note 
	SnappyData always initiates recovery for redundant, partitioned tables when a new SnappyData member joins the cluster or when a rebalance operation occurs.</p>

A value of -1, the default value, indicates that no recovery is performed unless a new SnappyData member joins the cluster, or unless individual tables override the recovery delay by specifying the `RECOVERYDELAY` clause in their `CREATE TABLE` statements. <mark> To Be Confirmed RowStore Docs See [Configuring High Availability for Partitioned Tables](http://rowstore.docs.snappydata.io/docs/data_management/configuring_ha_for_pr.html) </mark>.

## Default Value

-1

## Property Type

connection (boot)

## Prefix

snappydata.
