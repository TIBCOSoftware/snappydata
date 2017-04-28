# Disk Store Management

The `snappy `command-line tool has several options for examining and managing your disk stores. The `snappy` tool, along with the `CREATE DISKSTORE` statement, are your management tools for online and offline disk stores.

<a id="managing_disk_stores__section_4AFD4B9EECDA448BA5235FB1C32A48F1"></a>

!!!Note
	Each of these commands operates either on the online disk stores or offline disk stores, but not both. </p>

| SnappyData Command                | Online or Offline Command | See . . .  	|
|-----------------------------|---------------------------|-----------------------------------|
| `validate-disk-store`       | Off                       | [Validate a Disk Store](validating_disk_store.md)  |
| `compact-all-disk-stores`   | On                  | [Compacting Disk Store Log Files](compacting_disk_stores.md)|
| `compact-disk-store`        | Off                       | [Compacting Disk Store Log Files](compacting_disk_stores.md) |
| `backup`                    | On                        | [Backing Up and Restoring Disk Stores](backup_restore_disk_store.md) |
| `list-missing-disk-stores`  | On                        | [Handling Missing Disk Stores](handling_missing_disk_stores.md)|
| `revoke-missing-disk-store` | On                        | [Handling Missing Disk Stores](handling_missing_disk_stores.md) |

For the complete command syntax of any `snappy` command, run `snappy <command> --help` at the command line.

<a id="online-sd"></a>

## Online SnappyData Disk Store Operations

For online operations, `snappy` connects to a distributed system and sends the operation requests to the members that have disk stores. These commands do not run on offline disk stores. You must provide the command with a distributed system properties in a `gemfirexd.properties` file, or specify the multicast port or locator properties to connect to the cluster (for example, `-mcast-port=`*port_number*).

<a id="offline-sd"></a>

## Offline SnappyData Disk Store Operations

For offline operations, `snappy` runs the command against the specified disk store and its specified directories. You must specify all directories for the disk store.

Offline operations will not run on online disk stores. The tool locks the disk store while it is running, so the member cannot start in the middle of an operation.

If you try to run an offline command for an online disk store, you get a message like this:

``` pre
ERROR: Operation "validate-disk-store" failed because: disk-store=ds1: 
com.gemstone.gemfire.cache.DiskAccessException: For DiskStore: ds1: 
Could not lock "hostA/ds1dir1/DRLK_IFds1.lk". Other JVMs might have created 
diskstore with same name using the same directory., caused by 
java.io.IOException: The file "hostA/ds1dir1/DRLK_IFds1.lk" is being used 
by another process.
```

-   **[Validate a Disk Store](validating_disk_store.md)**
    The `snappy validate-disk-store` command verifies the health of your offline disk store. It gives you information about the tables in it, the total rows, and the number of records that would be removed if you compacted the store.

-   **[Compacting Disk Store Log Files](compacting_disk_stores.md)**
    You can configure automatic compaction for an operation log based on percentage of garbage content. You can also request compaction manually for online and offline disk stores.

-   **[Handling Missing Disk Stores](handling_missing_disk_stores.md)**
    Use the `snappy list-missing-disk-stores` command to find disk stores with the latest persisted data. Use `snappy revoke-missing-disk-stores` to stop waiting for the data when you cannot bring it online.

-   **[Managing How Data is Written to Disk](managing_disk_buffer_flushes.md)**
    You can configure SnappyData to write immediately to disk and you may be able to modify your operating system behavior to perform buffer flushes more frequently.


