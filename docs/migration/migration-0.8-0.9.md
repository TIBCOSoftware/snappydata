# Migrating from Version 0.8 to Version 0.9

!!! Note
	Upgrade of on-disk data files is not supported for this release. This document only contains instructions for users migrating from SnappyData 0.8 to SnappyData 0.9. After you have re-configured your cluster, you must reload your data into SnappyData tables.

## Memory Management: Heap and Off-Heap 
SnappyData can now be configured to use both off-heap and on-heap storage. The `memory-size` and `heap-size`  properties control the off-heap and on-heap sizes of the SnappyData server process. 

Row tables are always stored on on-heap storage. You can now configure column tables to use off-heap storage. Off-heap storage is also recommended for production environments. Several artifacts in the product, however, require on-heap memory, and therefore minimum heap size is also required in such cases. 
For example:

* To use row tables: According to the row table size requirements, configure the heap size. Currently, row tables in SnappyData do not use off-heap memory.

* To read-write Parquet and CSV: Parquet and CSV read-write are memory consuming activities, and still, use heap memory. Ensure that you provide sufficient heap memory in such cases.

* When most of your data reside in column tables, use off-heap memory. They are faster and put less pressure on garbage collection threads.

The following properties have been added for memory management:
<a id="memory-properties"></a>

| Properties | Description |
|--------|--------|
|`memory-size`|The total memory that can be used by the node for column storage and execution in off-heap. The default value is 0 (OFF_HEAP is not used by default).|
|`critical-heap-percentage`|The portion of memory that is reserved for system use, unaccounted on-the-fly objects, JVM, GC overhead etc. The default value is 90% and can be increased to 95% or similar for large heaps (>20G). This is only applicable to heap-size accounting. </br>Off-heap configuration with memory-size uses the entire available memory and does not have any reserved area.|
|`spark.memory.storageFraction`|The fraction of total storage memory that is immune to eviction. Execution can never grow into this portion of memory. </br>If set to a higher value, less working memory may be available for execution, and tasks may overflow to disk. It is recommended that you do not modify the default setting.|
|`spark.memory.storageMaxFraction`|Specifies how much off-heap memory can be consumed by storage. The default is 0.95 of the total `memory-size` (off-heap size). Beyond this, all data in storage gets evicted to disk. The split between execution and storage is governed by the `spark.memory.storageFraction` property, but storage grows into execution space up to this limit if space is available in the execution area. However, if execution requires space, then it evicts storage to the `spark.memory.storageFraction` limit. </br>Normally you do not need to modify this property even if queries are expected to take lots of execution space. It is better to use the `spark.memory.storageFraction` property to control the split between storage and execution.|

## Tables Persistent To Disk By Default 
In the previous releases (0.8 and earlier), tables were stored in memory by default, and users had to configure the `persistence` clause to store data on disk.
From this release onwards, all tables persist to disk by default and can be explicitly turned OFF for pure memory-only tables by specifying the `persistence` option as `none`.

## Changes to Properties
In this release, the following changes have been made to the properties 
* [Memory management](#memory-properties) 

* [New Properties](#new-properties)

* [Deleted Properties](#deleted-properties). 

Make sure that you familiarise yourself with changes before you re-configure your cluster.

<a id="new-properties"></a>
### New Property

| Property | Description |
|--------|--------|
|`snappydata.connection`|This property points to thrift network server running on a locator or a data server of a running SnappyData cluster. The value of this property is a combination of locator or server and JDBC client port on which the thrift network server listens for connections (The port that is specified by the `client-port` property and defaults to 1527 or the next available port on the locator/server).|

<a id="deleted-properties"></a>
### Deleted Property

| Property | Description |
|--------|--------|
|`snappydata.store.locators`|Instructs the connector to acquire cluster connectivity, catalog metadata and registers it locally in the Spark cluster.</br>This property has been deleted and replaced with ``snappydata.connection`.|
