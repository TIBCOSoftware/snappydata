#Overview
Tables in SnappyData can be partitioned or replicated. A replicated table keeps a copy of its entire data set locally on every SnappyData server in its server group. A partitioned table manages large volumes of data by partitioning it into manageable chunks and distributing those chunks across all members in the table’s server group.

By default, all tables are replicated unless you specify partitioning in the CREATE TABLE statement. The schema information for all SnappyData objects is visible at all times to all peer members of the distributed system including peer clients, but excluding standalone locators.

##Partitioned Tables
Horizontal partitioning involves spreading a large data set (many rows in a table) across members in a cluster. SnappyData uses a variant of the consistent hash algorithm to help ensure that data is uniformly balanced across all members of the target server group.

###How Table Partitioning Works 
You specify the partitioning strategy of a table in the PARTITION BY clause of the CREATE TABLE statement. The available strategies include hash-partitioning on each row’s primary key value, hash-partitioning on column values other than the primary key, range-partitioning, and list-partitioning.

SnappyData maps each row of a partitioned table to a logical “bucket.” The mapping of rows to buckets is based on the partitioning strategy that you specify. For example, with hash-partitioning on the primary key, SnappyData determines the logical bucket by hashing the primary key of the table. Each bucket is assigned to one or more members, depending on the number of copies that you configure for the table. Configuring a partitioned table with one or more redundant copies of data ensures that partitioned data remains available even if a member is lost.

When members are lost or removed, the buckets are reassigned to new members based on load. Losing a member in the cluster never results in re-assigning rows to buckets. You can specify the total number of buckets to use with the BUCKETS clause of the CREATE TABLE statement. The default number of buckets is 113.

In SnappyData, all peer servers in a distributed system know which peers host which buckets, so they can efficiently access a row with at most one network hop to the member that hosts the data. Reads or writes to a partitioned table are transparently routed to the server that hosts the row that is the target of the operation. Each peer maintains persistent communication channels to every peer in the cluster.

Although each bucket is assigned to one or more specific servers, you can use a procedure to relocate buckets in a running system, in order to improve the utilization of resources across the cluster. See Rebalancing Partitioned Data on SnappyData Members.

You can also pre-allocate buckets before loading data into the table, to ensure that imported data is evenly distributed among table partitions. See Pre-Allocating Buckets.

###Understanding Where Data Is Stored 
SnappyData uses a table’s partitioning column values and the partitioning strategy to calculate routing values (typically integer values). It uses the routing values to determine the “bucket” in which to store the data.

Each bucket is then assigned to a server, or to multiple servers if the partitioned table is configured to have redundancy. The buckets are not assigned when the table is started up, but occurs lazily when the data is actually put into a bucket. This allows you to start a number of members before populating the table.

If you set the redundant-copies for the table to be greater than zero, RowStore designates one of the copies of each bucket as the primary copy. All writes to the bucket go through the primary copy. This ensures that all copies of the bucket are consistent.

The Group Membership Service (GMS) and distributed locking service ensure that all distributed members have a consistent view of primaries and secondaries at any moment in time across the distributed system, regardless of membership changes initiated by the administrator or by failures.

###Failure and Redundancy 
If you have redundant copies of a partitioned table, you can lose servers without loss of data or interruption of service. When a server fails, SnappyData automatically re-routes any operations that were trying to write to the failed member to the surviving members.

RowStore also attempts to re-route failed read operations to another server if possible. If a read operation returns only a single row, then transparent failover is always possible. However, if an operation returns multiple rows and the application has consumed one or more rows, then RowStore cannot fail over if a server involved in the query happens goes offline before all the results have been consumed; in this case the application receives a SQLException with SQLState X0Z01. All applications should account for the possibility of receiving such an exception, and should manually retry the query if such a failure occurs..

Read operations are also retried if a server is unavailable when a query is performed. In this figure, M1 is reading table values W and Y. It reads W directly from its local copy and attempts to read Y from M3, which is currently offline. In this case, the read is automatically retried in another available member that holds a redundant copy of the table data.

###Rebalancing Partitioned Data on SnappyData Members 
You can use rebalancing to dynamically increase or decrease your SnappyData cluster capacity, or to improve the balance of data across the distributed system.

Rebalancing is a RowStore member operation that affects partitioned tables created in the cluster. Rebalancing performs two tasks:

* If the a partitioned table’s redundancy setting is not satisfied, rebalancing does what it can to recover redundancy. See Making a Partitioned Table Highly Available.

* Rebalancing moves the partitioned table’s data buckets between host members as needed to establish the best balance of data across the distributed system.

For efficiency, when starting multiple members, trigger the rebalance a single time, after you have added all members.

Start a rebalance operation using one of the following options:

* At the command line when you boot a RowStore server:

        snappy-shell rowstore server start -rebalance 
<mark> Command - To be modified</mark>
        

* Executing a system procedure in a running RowStore member:

        call sys.rebalance_all_buckets();
<mark> Command - To be modified</mark>

This procedure initiates rebalancing of buckets across the entire RowStore cluster for all partitioned tables.

###Managing Replication Failures 
SnappyData uses multiple failure detection algorithms to detect replication problems quickly. SnappyData replication design focuses on consistency, and does not allow suspect members or network-partitioned members to operate in isolation.


###Creating Partitioned Tables 
You [create a partitioned table](/programming_guide#markdown_link_row_and_column_tables) on a set of servers identified by named server groups (or on the default server group if you do not specify a named server group). Clauses in the CREATE TABLE statement determine how table data is partitioned, colocated, and replicated across the server group. 

## Replicated Tables
SnappyData server groups control which SnappyData data store members replicate the table’s data. SnappyData replicates table data both when a new table is initialized in a cluster and when replicated tables are updated.

###How SnappyData Replicates Tables 
SnappyData replicates every single row of a replicated table synchronously to each table replica in the target server group(s). With synchronous replication, table reads can be evenly balanced to any replica in the cluster, with no single member becoming a contention point.

####Replication at Initialization
When a non-persistent (“memory only”) replicated table is created in a peer or server, it initializes itself using data from another member of the server group that hosts the table. A single data store member in the server group is chosen, and data for the table is streamed from that member to initialize the new replica. If the selected member fails during the replication process, the initializing process selects a different member in the server group to strea

SnappyData replicates data to all peers in the server groups where the table was created. Replication occurs in parallel to other peers over a TCP channel.

SnappyData replicates table data both when a new table is initialized in a cluster and when replicated tables are updated.

####Replication During Updates
When an application updates a replicated table, SnappyData distributes the update to all replicas in parallel, utilizing the network bandwidth between individual members of the cluster. The sending peer or server locks the updated row locally, and then distributes the update to the replicas. After each of the replicas processes the update and responds with an acknowledgment message, the originating SnappyData peer returns control to the application. The update process is conservative in order to ensure that all copies of a replicated table contain consistent data. Each receiver processes the update entirely, applying constraint checks if necessary and updating the local table data, before responding with an acknowledgment. Each operation on a single row key is performed atomically per replica, regardless of how many columns are being read or updated.

###Deciding When to Use Replicated Tables 
Code tables are often good candidates for replication.

Application data is frequently normalized to maintain “code” fields in “fact” tables, and to maintain the details associated with each “code” in an associated “dimension” table. Code tables are often small and change infrequently, but they are frequently joined with their parent “fact” table in queries. Code tables of this sort are good candidates for using replicated tables.

Also note that this version of RowStore supports joins only on co-located data. Instead of using partitioning in all cases, you should consider having applications replicate smaller tables that are joined with other partitioned tables.

!!!Note
If multiple applications update the same row of a replicated table at the same time outside of a transaction, the table data can become out of sync when RowStore replicates those updates. Keep this limitation in mind when using replicated tables.

###Creating Replicated Tables 
You can [create replicated tables](/programming_guide#markdown_link_row_and_column_tables) explicitly or by default, using CREATE TABLE statement. <mark>Add link </mark>