# SYSDISKSTORES

Contains information about all disk stores created in the SnappyData distributed system.

See [CREATE DISKSTORE](../sql_reference/create-diskstore.md).

| Column Name          | Type    | Length | Nullable | Contents                                                                                                                             |
|----------------------|---------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------------|
| NAME                 | VARCHAR | 128    | No       | The unique identifier of the disk store.                                                                                             |
| MAXLOGSIZE           | BIGINT  | 10     | No       | The maximum size, in megabytes, of a single oplog file in the disk store.                                                            |
| AUTOCOMPACT          | CHAR    | 6      | No       | Specifies whether SnappyData automatically compacts log files in this disk store.                                                    |
| ALLOWFORCECOMPACTION | CHAR    | 6      | No       | Specifies whether the disk store permits online compaction of log files using the `snappy` utility. |
| COMPACTIONTHRESHOLD  | INTEGER | 10     | No       | The threshold after which an oplog file is eligible for compaction. Specified as a percentage value from 0–100.                      |
| TIMEINTERVAL         | BIGINT  | 10     | No       | The maximum number of milliseconds that can elapse before SnappyData asynchronously flushes data to disk.                            |
| WRITEBUFFERSIZE      | INTEGER | 10     | No       | The size of the buffer SnappyData uses to store operations when writing to the disk store.                                           |
| QUEUESIZE            | INTEGER | 10     | No       | The maximum number of row operations that SnappyData can asynchronously queue for writing to the disk store.                         |
| DIR_PATH_SIZE      | VARCHAR | 32672  | No       | The directory names that hold disk store oplog files, and the maximum size in megabytes that each directory can store.               |



