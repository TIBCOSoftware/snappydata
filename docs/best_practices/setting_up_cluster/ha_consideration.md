<a id="ha-consideration"></a>
## HA Considerations

High availability options are available for all the SnappyData components. 

**Lead HA** </br> 
SnappyData supports secondary lead nodes. If the primary lead becomes unavailable, one of  the secondary lead nodes takes over immediately. 
Setting up the secondary lead node is highly recommended because the system cannot function if the lead node is unavailable. Currently, the queries that are executing when the primary lead becomes unavailable, are not re-tried and have to be resubmitted.

**Locator**</br>  
SnappyData supports multiple locators in the cluster for high availability. 
It is recommended to set up multiple locators as, if a locator becomes unavailable, the cluster continues to be available. New members can however not join the cluster.
With multiple locators, clients notice nothing and the failover recovery is completely transparent.

**DataServer**</br> 
SnappyData supports redundant copies of data for fault tolerance. A table can be configured to store redundant copies of the data.  So, if a server is unavailable, and if there is a redundant copy available on some other server, the tasks are automatically retried on those servers. This is totally transparent to the user. 
However, the redundant copies double the memory requirements. If there are no redundant copies and a server with some data goes down, the execution of the queries fail and PartitionOfflineException is reported. The execution does not begin until that server is available again. 