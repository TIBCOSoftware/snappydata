# snappydata.default-startup-recovery-delay


## Description

The number of milliseconds to wait after a member joins the distributed system before recovering redundancy for partitioned tables. The default of 0 (zero) configures immediate redundancy recovery whenever a new partitioned table host joins. A value of -1 disables automatic recovery after a new member joins the distributed system. <!-- If both this property and [default-recovery-delay](default-recovery-delay.md) are set to -1, then no automatic redundancy recovery is performed when a new member joins the system. -->

<mark>See [Configuring High Availability for Partitioned Tables../../data_management/overview_how_pr_ha_works.md#how_pr_ha_works).</mark>

## Default Value

0

## Property Type

system

## Prefix

n/a
