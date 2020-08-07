# High Availability (HA) Considerations
<a id="ha-consideration"></a> <a id="cores"></a>

High availability options are available for all the TIBCO ComputeDB components.

**Lead** </br>
TIBCO ComputeDB supports secondary lead nodes. If the primary lead becomes unavailable, one of the secondary lead nodes takes over immediately. 
[Setting up the secondary lead node](../configuring_cluster/configuring_cluster.md#confsecondarylead) is highly recommended because the system cannot function if the lead node is unavailable. Currently, the queries and jobs that are executing when the primary lead becomes unavailable, are not re-tried and have to be resubmitted.

**Locator**</br>
TIBCO ComputeDB supports multiple locators in the cluster for high availability. 
It is recommended to set up multiple locators (ideally two). If a locator becomes unavailable, the cluster continues to be available. However, new members cannot join the cluster.</br>
With multiple locators, there are no impact on the clients and the failover recovery is completely transparent.

**DataServer**</br>
TIBCO ComputeDB supports redundant copies of data for fault tolerance. A table can be configured to store redundant copies of the data.  So, if a server is unavailable and if there is a redundant copy available on some other server, the tasks are automatically retried on those servers. This is totally transparent to the user. 
However, the redundant copies double the memory requirements. If there are no redundant copies and a server with some data goes down, the execution of the queries fail, and PartitionOfflineException is reported. The execution does not begin until that server is available again. 

## Known Limitation

In case of lead HA, the new primary lead node creates a new Snappy session for the JDBC clients. This means session specific properties (for example, `spark.sql.autoBroadcastJoinThreshold`, `snappydata.sql.hashJoinSize`) need to be set again after a lead failover.
