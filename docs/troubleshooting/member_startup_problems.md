<a id="member-startup-replay"></a>

# Member Startup Problems

When you start SnappyData members, startup delays can occur if specific disk store files on other members are unavailable. This is part of the normal startup behavior and is designed to help ensure data consistency. For example, consider the following startup message for a locator ("locator2):

```no-highlight
SnappyData Locator pid: 23537 status: waiting
Waiting for DataDictionary (DiskId: 531fc5bb-1720-4836-a468-3d738a21af63, Location: /snappydata/locator2/./datadictionary) on: 
 [DiskId: aa77785a-0f03-4441-84f7-6eb6547d7833, Location: /snappydata/server1/./datadictionary]
 [DiskId: f417704b-fff4-4b99-81a2-75576d673547, Location: /snappydata/locator1/./datadictionary]
```

Here, the startup messages indicate that locator2 is waiting for the persistent datadictionary files on locator1 and server1 to become available. SnappyData always persists the data dictionary for indexes and tables that you create, even if you do not configure those tables to persist their stored data. The startup messages above indicate that locator1 or locator2 might potentially store a newer copy of the data dictionary for the distributed system.

Continuing the startup by booting the server1 data store yields:

```no-highlight
Starting SnappyData Server using locators for peer discovery: localhost[10337],localhost[10338]
Starting network server for SnappyData Server at address localhost/127.0.0.1[1529]
Logs generated in /snappydata/server1/gfxdserver.log
The server is still starting. 15 seconds have elapsed since the last log message: 
 Region /_DDL_STMTS_META_REGION has potentially stale data. It is waiting for another member to recover the latest data.
My persistent id:

  DiskStore ID: aa77785a-0f03-4441-84f7-6eb6547d7833
  Name: 
  Location: /10.0.1.31:/snappydata/server1/./datadictionary

Members with potentially new data:
[
  DiskStore ID: f417704b-fff4-4b99-81a2-75576d673547
  Name: 
  Location: /10.0.1.31:/snappydata/locator1/./datadictionary
]
Use the "snappy-shell list-missing-disk-stores" command to see all disk stores that are being waited on by other members.
```

The data store startup messages indicate that locator1 has "potentially new data" for the data dictionary. In this case, both locator2 and server1 were shut down before locator1 in the system, so those members are waiting on locator1 to ensure that they have the latest version of the data dictionary.

The above messages for data stores and locators may indicate that some members were not started. If the indicated disk store persistence files are available on the missing member, simply start that member and allow the running members to recover. For example, in the above system you would simply start locator1 and allow locator2 and server1 to synchronize their data.

To avoid this type of delayed startup and recovery:

1.  It is recommended to use the built-in `snappy-start-all.sh` and `snappy-stop-all.sh` scripts to start and stop the cluster. If for some reason those scripts are not used, then when possible, first shut down the data store members after disk stores have been synchronized in the system.</br> Shut down remaining locator members after the data stores have stopped.

2.  Make that that sure all persistent members are restarted properly. See [Recovering from a ConflictingPersistentDataException](recovering_from_a_conflictingpersistentdataexception.md) for more information.

3.  If a member cannot be restarted and it is preventing other data stores from starting, then [revoke-missing-disk-store](../reference/command_line_utilities/store-revoke-missing-disk-stores.md) command can be used to revoke the disk stores that are preventing startup. 

	!!!Note
    	This can cause some loss of data if the revoked disk store actually contains recent changes to the data dictionary or to table data. The revoked disk stores cannot be added back to the system later. If you revoke a disk store on a member you need to delete the associated disk files from that member in order to start it again. Only use the `revoke-missing-disk-store` command as a last resort.  Contact [support@snappydata.io](mailto:support@snappydata.io) if you need to use the `revoke-missing-disk-store` command.