<a id="recovering-from-conflictingpersistentdataexception"></a>

# Recovering from a ConflictingPersistentDataException

If you receive a `ConflictingPersistentDataException` during startup, it indicates that you have multiple copies of some persistent data and TIBCO ComputeDB cannot determine which copy to use. Normally TIBCO ComputeDB uses metadata to automatically determine which copy of persistent data to use. Each member persists, along with the data dictionary or table data, a list of other members that have the data and whether their data is up to date.

A `ConflictingPersistentDataException` happens when two members compare their metadata and find that it is inconsistent—they either don’t know about each other, or they both believe that the other member has stale data. The following are some scenarios that can cause a `ConflictingPersistentDataException`.

**Independently-created copies**

Trying to merge two independently-created distributed systems into a single distributed system causes a `ConflictingPersistentDataException`. There are a few ways to end up with independently-created systems:

-   Configuration problems may cause TIBCO ComputeDB members connect to different locators that are not aware of each other. To avoid this problem, ensure that all locators and data stores always specify the same, complete list of locator addresses at startup (for example, `locators=locator1[10334],locator2[10334],locator3[10334]`). 

-   All persistent members in a system may be shut down, after which a brand new set of different persistent members attempts to start up.

Trying to merge independent systems by pointing all members to the same set of locators then results in a `ConflictingPersistentDataException`.

TIBCO ComputeDB cannot merge independently-created data for the same table. Instead, you need to export the data from one of the systems and import it into the other system. See Exporting and Importing Data with TIBCO ComputeDB

**Starting new members first**

Starting a brand new member with no persistent data before starting older members that have persistent data can cause a `ConflictingPersistentDataException`.

This can happen by accident if you shut down the system, then add a new member to the startup scripts, and finally start all members in parallel. In this case, the new member may start first. If this occurs, the new member creates an empty, independent copy of the data before the older members start up. When the older members start, the situation is similar to that described above in “Independently-created copies.”

In this case, the fix is simply to move aside or delete the (empty) persistence files for the new member, shut down the new member, and finally restart the older members. After the older members have fully recovered, restart the new member.

**A network split, with enable-network-partition-detection set to false**

With `enable-network-partition-detection` set to true, TIBCO ComputeDB detects a network partition and shuts down members to prevent a "split brain." In this case no conflicts should occur when the system is restored.

However, if `enable-network-partition-detection` is false, TIBCO ComputeDB cannot prevent a "split brain" after a network partition. Instead, each side of the network partition records that the other side of the partition has stale data. When the partition is healed and persistent members are restarted, they find a conflict because each side believes the other side's members are stale.

In some cases it may be possible to choose between sides of the network partition and keep only the data from one side of the partition. Otherwise you may need to salvage data and import it into a fresh system.

**Resolving a ConflictingPersistentDataException**

If you receive a `ConflictingPersistentDataException`, you will not be able to start all of your members and have them join the same distributed system.

First, determine if there is one part of the system that you can recover. For example, if you just added some new members to the system, try to start up without including those members. For the remaining members, use the data extractor tool to extract data from the persistence files and import it into a running system. 
