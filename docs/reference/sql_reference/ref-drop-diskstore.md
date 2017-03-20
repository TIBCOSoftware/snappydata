# DROP DISKSTORE

Remove a disk store configuration from the RowStore cluster.

##Syntax

``` pre
DROP DISKSTORE [ IF EXISTS ] store-name
```

**IF EXISTS   **
Include the `IF EXISTS` clause to execute the statement only if the specified disk store exists in RowStore.

**store-name   **
User-defined name of the disk store configuration that you want to remove. The available names are stored in the <a href="../system_tables/sysdiskstores.html#reference_36E65EC061C34FB696529ECA8ABC5BFC" class="xref noPageCitation" title="Contains information about all disk stores created in the RowStore distributed system.">SYSDISKSTORES</a> system table.

##Example

This command removes the disk store "STORE1" from the cluster:

``` pre
DROP DISKSTORE store1;
```


