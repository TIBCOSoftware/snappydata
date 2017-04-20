# Overview of Disk Stores

The two persistence options, overflow and persistence, can be used individually or together. Overflow uses disk stores as an extension of in-memory table management for both partitioned and replicated tables. Persistence stores a redundant copy of all table data in disk stores.

See [Evicting Table Data from Memory](../evicting_table_data/evicting_table_data_from_memory.md) for more information about configuring tables to overflow to disk.


## Shared-Nothing Disk Store Design

Individual SnappyData peers that host table data manage their own disk store files, completely separate from the disk stores files of any other member. When you create a disk store, you can define certain properties that specify where and how each SnappyData peer should manages disk store files on their local filesystem.

SnappyData supports persistence for replicated and partitioned tables. The disk store mechanism is designed for locally-attached disks. Each peer manages its own local disk store files, and does not share any disk artifacts with other members of the cluster. This shared nothing design eliminates the process-level contention that is normally associated with traditional clustered databases. Disk stores use rolling, append-only log files to avoid disk seeks completely. No complex B-Tree data structures are stored on disk; instead SnappyData always assumes that complex query navigation is performed using in-memory indexes.

Disk stores also support the SnappyData data rebalancing model. When you increase or decrease capacity by adding or removing peers in a cluster, the disk data also relocates itself as necessary.

## Data Types for Disk Storage

Disk storage can be used for persisting:

-   **Table data**. Persist and/or overflow table data managed in SnappyData peers.

-   **Gateway sender queues**. Persist gateway sender queues for high availability in a WAN deployment. These queues always overflow, and can be persistent.

-   **AsyncEventListener and DBSynchronizer queues.** Persist these queues for high availability. These queues always overflow, and can be persistent.

## Creating Disk Stores and Using the Default Disk Store

Create named disk stores in the data dictionary using the [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) DDL statement. You can then specify named disk stores for individual tables in the [CREATE TABLE](../../../reference/sql_reference/create-table/) DDL statements for persistence and/or overflow. You can store data from multiple tables and queues in the same named disk store. See [Guidelines for Designing Disk Stores](using_disk_stores.md).

Tables that do not name a disk store but specify persistence or overflow in their `CREATE TABLE` statement use the default disk store. The location of the default diskstore is determined by the value of the `sys-disk-dir` boot property. The default disk store is named <mark>GFXD-DEFAULT-DISKSTORE. </br>TO BE CONFIRMED</mark>

<mark>Gateway sender queues, AsyncEventListener queues, and DBSynchronizer queues can also be configured to use a named disk store. The default disk store is used if you do not specify a named disk store when creating the queue. See CREATE GATEWAYSENDER</mark> or <mark>CREATE ASYNCEVENTLISTENER</mark>.

## Peer Client Considerations for Persistent Data

Peer clients (clients started using the `host-data=false` property) do not use disk stores and can never persist the SnappyData data dictionary. Instead, peer clients rely on other data stores or locators in the distributed system for persisting data. If you use a peer client to execute DDL statements that require persistence and there are no data stores available in the distributed system, SnappyData throws a data store unavailable exception (SQLState: X0Z08).

You must start locators and data stores *before* starting peer clients in your distributed system. If you start a peer client as the first member of a distributed system, the client initializes an empty data dictionary for the distributed system as a whole. Any subsequent datastore that attempts to join the system conflicts with the empty data dictionary and fails to start with a `ConflictingPersistentDataException`.

-   **[What SnappyData Writes to the Disk Store](disk-store-contents.md)**
    For each disk store, SnappyData stores detailed information about related members and tables.

-   **[Disk Store State](disk-store-state.md)**
    Disk store access and management differs according to whether the store is online or offline.

-   **[Disk Store Directories](disk_store_directories.md)**
    When you create a disk store, you can optionally specify the location of directories where SnappyData stores persistence-related files.

-   **[Disk Store Persistence Attributes](disk_store_persistence_modes.md)**
    SnappyData persists data on disk stores in synchronous or asynchronous mode.

-   **[Disk Store File Names and Extensions](file_names_and_extensions.md)**
    Disk store files include store management and access control files and the operation log, or oplog, files, consisting of one file for deletions and another for all other operations.

-   **[Disk Store Operation Logs](operation_logs.md)**
    At creation, each operation log is initialized at the disk store's MAXLOGSIZE value, with the size divided between the `crf` and `drf` files. SnappyData only truncates the unused space on a clean shutdown (for example, `snappy rowstore server stop` or `snappy shut-down-all`).

-   **[Factors Contributing to High Disk Throughput](disk-store-throughput.md)**
    SnappyData disk store design contains several factors that contribute to very high disk throughput. They include pooling, avoiding seeks, and buffered logging.
