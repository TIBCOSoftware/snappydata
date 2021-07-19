# modify-disk-store


Modifies the content stored in a disk store.

!!! Caution 
	This operation writes to the disk store files. Hence you must use this utility cautiously.

## Syntax

**For secured cluster** 

```
Snappy>create region --name=regionName --type=PARTITION_PERSISTENT_OVERFLOW
```

**For non-secured cluster**

## Description

The following table describes the options used for `snappy modify-disk-store`:

| Items | Description |
|--------|--------|
|     -region   |    Specify the name of the region.  |
|     -remove   |    This option removes the region from the disk store. All the data stored in the disk store for this region will no longer exist if you use this option. |
|    -statisticsEnabled | Enables the region's statistics. Values are true or false. |

!!! Note
	The name of the disk store, the directories its files are stored in, and the region to target are all required arguments.

## Examples 

**Secured cluster**

**Non-secured cluster**


