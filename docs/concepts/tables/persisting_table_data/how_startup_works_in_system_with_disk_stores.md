# Recovering Persistent Data at Startup

When you shut down a member that persists data, the data remains in the disk store files, available to be reloaded when the member starts up again. Keep in mind that peer clients are dependent on locators or data store members to persist data, as they cannot persist data on their own.



The following sections explain what happens during startup and shutdown:


<a id="shutdown"></a>
## Shutdown: Most Recent Data from the Last Run

-   [Shutdown: Most Recent Data from the Last Run](#shutdown)

-   [Startup Process](#startup-process)

-   [Example Startup Scenarios](#example-startup)

If more than one member has the same persistent table or queue, the last member to exit leaves the most up-to-date data on disk.

SnappyData stores information on member exit order in the disk stores, so it can start your members with the most recent data set:

-   For a persistent replicated table, the last member to exit leaves the most recent data on disk.

-   For a partitioned table, where the data is split into buckets, the last member to exist that hosts a particular bucket leaves the most recent data on disk for that bucket.

!!!Note: 
	Peer clients rely on data stores for persistence. [Peer Client Considerations for Persistent Data](how_disk_stores_work.md#how_disk_stores_work__section_1A93EFBE3E514918833592C17CFC4C40).

<a id="startup-process"></a>

## Startup Process

When you start a member that uses disk stores, the persisted data is loaded back into memory to initialize tables and queues. If the member does not hold all of the most recent data for the system:

1.  The member does not immediately join the server group, but instead waits for the member with the most recent data.

    If your log level is set to "info" or below, the system provides messaging about the wait. In this example, the disk store for hostA has the most recent data and hostB is waiting for it.

	    [info 2010/04/09 10:48:26.039 PDT CacheRunner <main> tid=0x1]  
    	Region /persistent_PR initialized with data from 
    	/10.80.10.64:/export/straw3/users/jpearson/GemFireTesting/hostB/
    	backupDirectory created at timestamp 1270834766425 version 0 is     
    	waiting for the data previously hosted at 
    	[/10.80.10.64:/export/straw3/users/jpearson/GemFireTesting/hostA/
    	backupDirectory created at timestamp 1270834763353 version 0] to 
    	be available
    

    During normal startup you can expect to see some waiting messages.

2.  When the most recent data is available, the system updates the local tables as needed, logs a message like this, and continues with startup.
	
    	[info 2010/04/09 10:52:13.010 PDT CacheRunner <main> tid=0x1]    
       	Done waiting for the remote data to be available.
    
Each member’s persistent tables load and go online as quickly as possible, not waiting unnecessarily for other members to complete. For performance reasons, several actions are taken asynchronously:

-   If both primary *and* secondary buckets are persisted, data is made available when the primary buckets are loaded without waiting for the secondary buckets to load. The secondary buckets load asynchronously.

-   Entry keys first get loaded from the key file if this file is available (see information about the `krf` file in <mark>Disk Store File Names and Extensions TO BE CONFIRMED </mark>). Once all keys are loaded, SnappyData loads the entry values asynchronously. If a value is requested before it is loaded, the value is immediately fetched from the disk store.

<a id="example-startup"></a>

## Example Startup Scenarios

-   Stop order for a replicated, persistent table:
    1.  Member A (MA) exits first, leaving persisted data on disk for TableP.

    2.  Member B (MB) continues to run DML operations on TableP, which update its disk store and leaves the disk store for MA in a stale condition.

    3.  MB exits, leaving the most up-to-date data on disk for Table P.

-   Restart order Scenario 1:
    1.  MB is started first. SnappyData recognizes MB as having the most recent disk data for TableP and initializes it from disk.

    2.  MA is started, recovers its data from disk, and updates it as needed from the data in MB.

-   Restart order Scenario 2:
    1.  MA is started first. SnappyData recognizes that MA does not have the most recent disk store data and waits for MB to start before creating TableP in MA.

    2.  MB is started. SnappyData recognizes MB as having the most recent disk data for TableP and initializes it from disk.

    3.  MA recovers its TableP data from disk and updates it as needed from the data in MB.

-   **[Start a System with Disk Stores](starting_system_with_disk_stores.md)**
    When you start a SnappyData cluster with disk stores, it is recommended that you start all members that have persisted data at roughly the same time. Also enable network partition detection to avoid persisting inconsistent data during segmentation.


