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

If you set the redundant-copies for the table to be greater than zero, SnappyData designates one of the copies of each bucket as the primary copy. All writes to the bucket go through the primary copy. This ensures that all copies of the bucket are consistent.

The Group Membership Service (GMS) and distributed locking service ensure that all distributed members have a consistent view of primaries and secondaries at any moment in time across the distributed system, regardless of membership changes initiated by the administrator or by failures.

###Failure and Redundancy 
If you have redundant copies of a partitioned table, you can lose servers without loss of data or interruption of service. When a server fails, SnappyData automatically re-routes any operations that were trying to write to the failed member to the surviving members.

SnappyData also attempts to re-route failed read operations to another server if possible. If a read operation returns only a single row, then transparent failover is always possible. However, if an operation returns multiple rows and the application has consumed one or more rows, then SnappyData cannot fail over if a server involved in the query happens goes offline before all the results have been consumed; in this case the application receives a SQLException with SQLState X0Z01. All applications should account for the possibility of receiving such an exception, and should manually retry the query if such a failure occurs..

Read operations are also retried if a server is unavailable when a query is performed. In this figure, M1 is reading table values W and Y. It reads W directly from its local copy and attempts to read Y from M3, which is currently offline. In this case, the read is automatically retried in another available member that holds a redundant copy of the table data.

###Rebalancing Partitioned Data on SnappyData Members 
You can use rebalancing to dynamically increase or decrease your SnappyData cluster capacity, or to improve the balance of data across the distributed system.

Rebalancing is a SnappyData member operation that affects partitioned tables created in the cluster. Rebalancing performs two tasks:

* If the a partitioned table’s redundancy setting is not satisfied, rebalancing does what it can to recover redundancy. See Making a Partitioned Table Highly Available.

* Rebalancing moves the partitioned table’s data buckets between host members as needed to establish the best balance of data across the distributed system.

For efficiency, when starting multiple members, trigger the rebalance a single time, after you have added all members.

Start a rebalance operation using one of the following options:

* At the command line when you boot a SnappyData server:

        snappy SnappyData server start -rebalance 
<mark> Command - To be modified</mark>
        

* Executing a system procedure in a running SnappyData member:

        call sys.rebalance_all_buckets();
<mark> Command - To be modified</mark>

This procedure initiates rebalancing of buckets across the entire SnappyData cluster for all partitioned tables.

### Managing Replication Failures 
SnappyData uses multiple failure detection algorithms to detect replication problems quickly. SnappyData replication design focuses on consistency, and does not allow suspect members or network-partitioned members to operate in isolation.

#### Configuring suspect-member Alerts

When any member of the distributed system fails, it is important for other services to detect the loss quickly and transition application clients to other members. Any peer or server in the cluster can detect a problem with another member of the cluster, which initiates "SUSPECT" processing with the membership coordinator. The membership coordinator then determines whether the suspect member should remain in the distributed system or should be removed.

Use the `ack-wait-threshold` property to configure how long a SnappyData peer or server waits to receive an acknowledgment from other members that are replicating a table's data. The default value is 15 seconds; you specify a value from 0 to 2147483647 seconds. After this period, the replicating peer sends a severe alert warning to other members in the distributed system, raising a "suspect_member" alert in the cluster.

To configure how long the cluster waits for this alert to be acknowledged, set the `ack-severe-alert-threshold` property. The default value is zero, which disables the property.

#### How Replication Failure Occurs

Failures during replication can occur in the following ways:

-   A replica fails before sending an acknowledgment.

    The most common failure occurs when a member process is terminated during replication. When this occurs, the TCP connection from all members is terminated, and the membership view is updated quickly to reflect the change. The member who initiated replication continues replicating to other members.

    If instead of terminating, the process stays alive (but fails to respond) the initiating member waits for a period of time and then raises an alert with the distributed system membership coordinator. The membership coordinator evaluates the health of the suspect member based on heartbeats and health reports from other members in the distributed system. The coordinator may decide to evict the member from the distributed system, in which case it communicates this change in the membership view to all members. At this point, the member that initiated replication proceeds and completes replication using available peers and servers. In addition, clients connected to this member are automatically re-routed to other members.

-   An "Owning" member fails.

    If the designated owner of data for a certain key fails, the system automatically chooses another replica to become the owner for the key range that the failed member managed. The updating thread is blocked while this transfer takes place. If at least one replica is available, the operations always succeeds from the application's viewpoint.


### Creating Partitioned Tables 
You [create a partitioned table](../../pgm_guide/tables_in_snappydata.md) on a set of servers identified by named server groups (or on the default server group if you do not specify a named server group). Clauses in the CREATE TABLE statement determine how table data is partitioned, colocated, and replicated across the server group.
