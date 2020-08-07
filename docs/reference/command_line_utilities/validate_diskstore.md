# validate-disk-store

Verifies the health of an offline disk store and provides information about the tables using that disk store.

## Syntax

```
./bin/snappypy validate-disk-store <diskStoreName> <directory>+
```
In the syntax, you must provide the name of disk store to be validated and the path which stores the disk store files.

## Description

The TIBCO ComputeDB **validate-disk-store **command verifies the health of your offline disk store and gives you information about the following:
*	Tables that are using that disk store
*	Total number of rows 
*	Number of records that would be removed, if you have compacted the store.

You can use this command:

*	Before compacting an offline disk store, to determine whether it is worthwhile.
*	Before restoring a disk store.
*	Any time you want to ensure the disk store is in good shape.

## Example

```
 ./bin/snappy validate-disk-store GFXD-DEFAULT-DISKSTORE /home/xyz/<snappydata_install_dir>/work/localhost-server-1
```

This command displays an output  as shown in the following example:

```
log4j:WARN No appenders could be found for logger (org.eclipse.jetty.util.log).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
/__UUID_PERSIST: entryCount=42
/APP/SNAPPYSYS_INTERNAL____AIRLINE_COLUMN_STORE_ entryCount=0 bucketCount=8
/partitioned_region entryCount=6 bucketCount=10
Disk store contains 1 compactable records.
Total number of region entries in this disk store is: 6
```

