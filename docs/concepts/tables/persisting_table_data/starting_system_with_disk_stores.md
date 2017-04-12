# Start a System with Disk Stores

When you start a SnappyData cluster with disk stores, it is recommended that you start all members that have persisted data at roughly the same time. Also enable network partition detection to avoid persisting inconsistent data during segmentation.

<a id="starting_system_with_disk_stores__section_D0A7403707B847749A22BF9221A2C823"></a>
**Procedure**

1.  **Start members with persisted data at the same time**.

    When members with persistent data boot, they determine which have the most recent table data, and they initialize their caches using that data. If you do not start persistent data stores in parallel, some members may hang while waiting for other members to start.

    The following example bash script starts members in parallel. The script waits for the startup to finish and exits with an error status if one of the jobs fails.

        #!/bin/bash

        # Start all local SnappyData members to waiting state, regardless of which member holds the most recent
        # disk store files:

        ssh servera "snappy rowstore locator start -dir=/locator1 -sync=false -enable-network-partition-detection=true"
        ssh serverb "snappy rowstore server start -client-port=1528 -locators=localhost[10334] -dir=/server1 -sync=false -enable-network-partition-detection=true"
        ssh serverc "snappy rowstore server start -client-port=1529 -locators=localhost[10334] -dir=/server2 -sync=false -enable-network-partition-detection=true"

        # Wait until all members have finished synchronizing and starting:

        ssh servera "snappy rowstore locator wait -dir=/locator1"
        ssh serverb "snappy rowstore server wait -dir=/server1"
        ssh serverc "snappy rowstore server wait -dir=/server2"

        # Continue any additional tasks that require access to the SnappyData members...

        [...]

2.  **Respond to any member startup hangs**. If a disk store with the most recent table data does not come online, other members wait indefinitely rather than come online with stale data. Check for missing disk stores with the `snappy list-missing-disk-stores` command.
    1.  If no disk stores are missing, your peer initialization may be slow for some other reason. Check to see if other members are hanging or fail to start.

    2.  If disk stores are missing that you think should be there:

        1.  Make sure you have started the member. Check the logs for any failure messages.

        2.  Make sure your disk store files are accessible. If you have moved your member or disk store files, you must update your disk store configuration to match.

    3.  If disk stores are missing that you know are lost, because you have deleted them or their files are otherwise unavailable, revoke them so the startup can continue.

See [Handling Missing Disk Stores](../../../concepts/tables/disk_storage/handling_missing_disk_stores/) for more information.


