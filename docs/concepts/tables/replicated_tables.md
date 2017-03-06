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