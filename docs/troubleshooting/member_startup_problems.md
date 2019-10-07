<a id="member-startup-replay"></a>

# Member Startup Problems

This section provides information and resolutions for the issues faced during the startup of cluster members. </br>


The following issues are included here:

*	[Delayed startup due to unavailable disk stores](#delayedstartup)
*	[Delayed startup due to missing disk stores](#missingdiskstore)	

To avoid delayed startup and recovery, the following actions are recommended:

1.  Use the built-in `snappy-start-all.sh` and `snappy-stop-all.sh` scripts to start and stop the cluster. If for some reason those scripts are not used, then when possible, first shut down the data store members after disk stores have been synchronized in the system.</br> Shut down remaining locator members after the data stores have stopped.

2.  Ensure that all persistent members are restarted properly. See [Recovering from a ConflictingPersistentDataException](recovering_from_a_conflictingpersistentdataexception.md) for more information.

<a id= delayedstartup> </a>
## Delayed Startup Due to Unavailable Disk Stores

When you start SnappyData members, startup delays can occur if specific disk store files on other members are unavailable. This is part of the normal startup behavior and is designed to help ensure data consistency. For example, consider the following startup message for a locator (**locator2**):

```pre
SnappyData Locator pid: 23537 status: waiting
Waiting for DataDictionary (DiskId: 531fc5bb-1720-4836-a468-3d738a21af63, Location: /snappydata/locator2/./datadictionary) on: 
 [DiskId: aa77785a-0f03-4441-84f7-6eb6547d7833, Location: /snappydata/server1/./datadictionary]
 [DiskId: f417704b-fff4-4b99-81a2-75576d673547, Location: /snappydata/locator1/./datadictionary]
```

Here, the startup messages indicate that **locator2** is waiting for the persistent datadictionary files on **locator1** and **server1** to become available. SnappyData always persists the data dictionary for indexes and tables that you create, even if you do not configure those tables to persist their stored data. The startup messages above indicate that **locator1** or **locator2** might potentially store a newer copy of the data dictionary for the distributed system.

Continuing the startup by booting the **server1** data store yields:

```pre
Starting SnappyData Server using locators for peer discovery: localhost[10337],localhost[10338]
Starting network server for SnappyData Server at address localhost/127.0.0.1[1529]
Logs generated in /snappydata/server1/gfxdserver.log
The server is still starting. 15 seconds have elapsed since the last log message: 
 Region /_DDL_STMTS_META_REGION has potentially stale data. It is waiting for another member to recover the latest data.
My persistent id:

  DiskStore ID: aa77785a-0f03-4441-84f7-6eb6547d7833
  Name: 
  Location: /10.0.1.31:/snappydata/server1/./datadictionary
up
Members with potentially new data:
[
  DiskStore ID: f417704b-fff4-4b99-81a2-75576d673547
  Name: 
  Location: /10.0.1.31:/snappydata/locator1/./datadictionary
]
Use the "snappy-shell list-missing-disk-stores" command to see all disk stores that are being waited on by other members.
```

The data store startup messages indicate that **locator1** has "potentially new data" for the data dictionary. In this case, both **locator2** and **server1** were shut down before **locator1** in the system, so those members are waiting on **locator1** to ensure that they have the latest version of the data dictionary.

The above messages for data stores and locators may indicate that some members were not started. If the indicated disk store persistence files are available on the missing member, simply start that member and allow the running members to recover. For example, in the above system you would simply start locator1 and allow **locator2** and **server1** to synchronize their data.

<a id= missingdiskstore> </a>
## Delayed Startup Due to Missing Disk Stores

Sometimes a cluster does not get started, if the disk store files are missing from one of servers in the cluster. 
For example, you start a cluster that consists of **server1** and **server2**. Suppose the disk store files in **server1** are unavailable due to corruption or deletion. </br>**server1**, where the files were missing, attempts to start up as a new member, but it cannot due to InternalGemFireError and **server2** cannot start because it is waiting for the missing disk stores on **server1**. </br>In such a case, you can unblock the waiting server.
In case of more than two servers, despite of unblocking the waiting diskstores, one server can be still waiting upon the dependent server to come up. In such a case, change the order of the servers in the **conf** file and then restart the cluster.

### Unblocking the Disk Store

In the following sample startup log message that is displayed, you are notified that the **server1** which has process ID number (PID) 21474 cannot come up because it joined the cluster as a new member and **server2** with PID 21582 is waiting for the missing disk stores on **server1**.</br>
```Pre
SnappyData Server pid: 21474 status: stopped
Error starting server process: 
InternalGemFireError: None of the previous persistent node is up. - See log file for details.
SnappyData Server pid: 21582 status: waiting
Member disk state is not the most current. Table __PR._B__APP_ADJUSTMENT_4 at location /home/xyz/snappy/snappydata/server1/snappy-internal-delta is waiting for disk recovery from following members: 
[/127.0.0.1] [DiskId: 6190c93b-158f-40f1-8251-1e9c58e320c2, Location: /home/xyz/snappy/snappydata/server0/snappy-internal-delta]
```
Execute the  `./bin/snappy list-missing-disk-stores locators=<host>:<port>` command to view all the disk stores that are missing and awaited upon by other servers in the cluster. Check the messages in the **start_snappyserver.log** file of the server which is waiting for the missing disk stores. 

The following sample message is displayed in the **start_snappyserver.log** file for the servers:</br>
```Pre
[info 2018/07/10 12:00:16.302 IST <Recovery thread for bucket _B__APP_ADJUSTMENT_4> tid=0x4c] Region /APP/SNAPPYSYS_INTERNAL____ADJUSTMENT_COLUMN_STORE_, bucket 4 has potentially stale data.  It is waiting for another member to recover the latest data.
  My persistent id:
    DiskStore ID: 93e7e9cf-a513-4c67-89c3-da7e94a08efb
    Location: /127.0.0.1:/home/xyz/snappy/snappydata/server2
  Members with potentially new data:
  [
    DiskStore ID: 9791b9ff-7df3-44e8-99c8-7d62a3387002
    Location: /127.0.0.1:/home/xyz/snappy/snappydata/server1
  ]
``` 
In this sample log message, the diskID of **server2** is waiting upon the diskID of **server1**, which is missing. To start **server2**, the waiting disk store in that server must be unblocked.
Here it is shown that the diskID *93e7e9cf-a513-4c67-89c3-da7e94a08efb* of **server2** is waiting upon the diskID *9791b9ff-7df3-44e8-99c8-7d62a3387002* of **server1**, which is missing.



Run the `unblock-disk-store` utility, in the following format, to unblock the waiting disk store:</br>
`./bin/snappy unblock-disk-store <diskID of the waiting server>locators=localhost:10334`</br>
For example, `./bin/snappy unblock-disk-store 93e7e9cf-a513-4c67-89c3-da7e94a08efb -locators=localhost:10334`

Restart the cluster and keep unblocking such disk stores that are displayed in the logs until all the servers reach the running status.

!!!Note
	There is no loss of data when you unblock the disk stores.

### Rebalancing Data on Servers

After unblocking the disk stores, if you notice that one of the server in the cluster has more data as compared to the other servers, you can distribute the data among the servers. This ensures that each server carries almost equal data. To balance the data equally on the servers, do the following:

1.	Connect to snappy shell and obtain the jdbc client connection.
2.	Run the rebalance command.</br>
`snappy> call sys.rebalance_all_buckets();`

## Revoking Disk Stores that Prevent Startup

If a member cannot be restarted even after unblocking the disk store and restarting after re-ordering the servers in the **conf** file,  only then use the [revoke-missing-disk-store](../reference/command_line_utilities/store-revoke-missing-disk-stores.md) command.

!!!Caution
    	This can cause some loss of data if the revoked disk store actually contains recent changes to the data dictionary or to table data. The revoked disk stores cannot be added back to the system later. If you revoke a disk store on a member you need to delete the associated disk files from that member in order to start it again. Only use the `revoke-missing-disk-store` command as a last resort.  Contact [support@snappydata.io](mailto:support@snappydata.io) if you need to use the `revoke-missing-disk-store` command.