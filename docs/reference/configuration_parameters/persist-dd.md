# persist-dd

## Description

Enables or disables disk persistence for the data dictionary. By default, all data stores (TIBCO ComputeDB members booted with host-data=true) set this value to "true" to enable persistence. All TIBCO ComputeDB data stores in the same cluster must use the same persistence setting.

When persistence is enabled, if all data stores in a cluster are down, then clients cannot execute DDL statements in the cluster until a data store becomes available. This ensures that the persisent data dictionary can be recovered when the final data store rejoins the cluster.


!!! Note 
	You cannot enable persistence for TIBCO ComputeDB accessors (members that booted with host-data=false). </p>

When persist-dd is set to "false," then no tables can be declared as persistent. However, overflow can still be configured if you explicitly define the `sys-disk-dir` attribute.

When a new TIBCO ComputeDB member joins an existing cluster, the data dictionary is obtained either from other members, or it is retrieved from persisted data if the new member is determined to have to most current copy of the data in the cluster.

<!--[Optimizing a System with Disk Stores](../../concepts/tables/persisting_table_data/running_system_with_disk_stores.md#running_system_with_disk_stores) provides more information about starting and shutting down TIBCO ComputeDB clusters that utilize disk stores for persistence.
-->
## Default Value

true

## Property Type

connection (boot)

## Prefix

snappydata.
