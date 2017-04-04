# Validate a Disk Store

The `snappy validate-disk-store` command verifies the health of your offline disk store. It gives you information about the tables in it, the total rows, and the number of records that would be removed if you compacted the store.

<a id="validating_disk_store__section_1782CD93DB6040A2BF52014A6600EA44"></a>
When to use this command:

-   Before compacting an offline disk store to help decide whether it’s worth doing.

-   Before restoring a disk store.

-   Any time you want to be sure the disk store is in good shape.

<a id="validating_disk_store__section_718CE2F437B1447FAE2EE0619300606F"></a>

## Example


``` pre
snappy validate-disk-store ds1 hostB/bupDirectory
/partitioned_table entryCount=6 bucketCount=10
Disk store contains 1 compactable records.
Total number of table entries in this disk store is: 6
```
