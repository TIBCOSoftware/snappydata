# DROP DISKSTORE

Removes a disk store configuration from the SnappyData cluster.

```pre
DROP DISKSTORE [ IF EXISTS ] store-name
```

**IF EXISTS** </br>
Include the `IF EXISTS` clause to execute the statement only if the specified disk store exists in SnappyData.

**store-name**</br>
User-defined name of the disk store configuration that you want to remove. The available names are stored in the [SYSDISKSTORES](../../reference/system_tables/sysdiskstores.md) system table.

## Example

This command removes the disk store "STORE1" from the cluster:

```pre
DROP DISKSTORE store1;
```

**Related Topics**

* [CREATE DISKSTORE](../sql_reference/create-diskstore.md)

* [SYSDISKSTORES](../system_tables/sysdiskstores.md)
