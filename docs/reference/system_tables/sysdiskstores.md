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


**Example** </br>

```pre
snappy> select * from sys.SYSDISKSTORES;
NAME                       |MAXLOGSIZE          |AUTOCOMPACT|ALLOWFORCECOMPACTION|COMPACTIONTHRESHOLD|TIMEINTERVAL        |WRITEBUFFERSIZE|QUEUESIZE  |DIR_PATH_SIZE              
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
STORE2                     |456                 |true       |false               |50                 |1000                |19292393       |17374      |/build-artifacts/scala-2.11/snappy/&
STORE1-SNAPPY-DELTA        |50                  |true       |false               |80                 |223344              |19292393       |17374      |/build-artifacts/scala-2.11/snappy/&
STORE2-SNAPPY-DELTA        |50                  |true       |false               |50                 |1000                |19292393       |17374      |/build-artifacts/scala-2.11/snappy/&
GFXD-DEFAULT-DISKSTORE     |1024                |true       |false               |50                 |1000                |32768          |0          |/build-artifacts/scala-2.11/snappy/&
GFXD-DD-DISKSTORE          |10                  |true       |false               |50                 |1000                |32768          |0          |/build-artifacts/scala-2.11/snappy/&
SNAPPY-INTERNAL-DELTA      |50                  |true       |false               |50                 |1000                |32768          |0          |/build-artifacts/scala-2.11/snappy/&
STORE1                     |1024                |true       |false               |80                 |223344              |19292393       |17374      |/build-artifacts/scala-2.11/snappy/&

7 rows selected
```

```pre
snappy> select * from SYS.SYSDISKSTORES where NAME = 'STORE1';
NAME                       |MAXLOGSIZE          |AUTOCOMPACT|ALLOWFORCECOMPACTION|COMPACTIONTHRESHOLD|TIMEINTERVAL        |WRITEBUFFERSIZE|QUEUESIZE  |DIR_PATH_SIZE              
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
STORE1                     |1024                |true       |false               |80                 |223344              |19292393       |17374      |/build-artifacts/scala-2.11/snappy/&

1 row selected
```

```pre
snappy> select * from sys.sysdiskstores where DIR_PATH_SIZE like '%mytest%';
NAME             |MAXLOGSIZE          |AUTOCOMPACT|ALLOWFORCECOMPACTION|COMPACTIONTHRESHOLD|TIMEINTERVAL        |WRITEBUFFERSIZE|QUEUESIZE  |DIR_PATH_SIZE
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
TEST             |1024                |true       |false               |50                 |1000                |32768          |0          |build-artifacts/scala-2.11/snappy/work/localhost-server-1/mytest                      
TEST-SNAPPY-DELTA|50                  |true       |false               |50                 |1000                |32768          |0          |build-artifacts/scala-2.11/snappy/work/localhost-server-1/mytest/snappy-internal-delta

2 rows selected
```

**Related Topics**

* [DROP DISKSTORE](../sql_reference/drop-diskstore.md)

* [CREATE DISKSTORE](../sql_reference/create-diskstore.md)
