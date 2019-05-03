# SYSDISKSTOREIDS

Contains information about all disk stores IDs created in the TIBCO ComputeDB distributed system.

| Column Name          | Type    | Length | Nullable | Contents|
|----------------------|---------|--------|----------|--------------------------|
|MEMBERID | VARCHAR|128 |false |The ID of the cluster member.|
|NAME| VARCHAR|128| false|The diskstore name. Two inbuilt diskstores one for DataDictionary is named `GFXD-DD-DISKSTORE` while the default data diskstore is named `GFXD-DEFAULT-DISKSTORE`.|
|ID|CHAR|36| false|The unique diskstore ID. This is what appears in log files or output of `snappy-status-all.sh` script if a member is waiting for another node to start to sync its data (if it determines the other node may have more recent data). |
|DIRS| VARCHAR|32672 |false |Comma-separated list of directories used for the diskstore. These are the ones provided in [CREATE DISKSTORE](../sql_reference/create-diskstore.md) or else if no explicit directory was provided, then the working directory of the node. |

**Example** </br>

```pre
snappy> create diskstore d1 ('D1');
snappy> select * from sys.diskstoreids;
MEMBERID                        |NAME                            |ID                              |DIRS                            
-----------------------------------------------------------------------------------------------------------------------------------
127.0.0.1(7794)<v2>:20960       |GFXD-DEFAULT-DISKSTORE          |a47f63ca-f128-4102-bcd6-51549fa&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |STORE1                          |88d7685c-a190-4711-b2ca-ec27ece&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |D1-SNAPPY-DELTA                 |7432070a-05e3-4303-b731-91abbe3&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |GFXD-DEFAULT-DISKSTORE          |d1024174-8ec8-47ac-8df8-4b7f63b&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |STORE1-SNAPPY-DELTA             |e252410f-1440-4b8e-9b84-343276f&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |D1                              |56a1f778-ba8c-41bd-8556-27c35fa&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |STORE2-SNAPPY-DELTA             |fc3a72c0-f70a-4fed-891f-f3521d4&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |STORE2                          |4b089853-3fb1-4fd7-ba81-e3e6fe3&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |GFXD-DD-DISKSTORE               |72350926-f5fa-415f-9124-24e0944&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7638)<v1>:24353       |SNAPPY-INTERNAL-DELTA           |6a20b9db-e5c6-4081-baa8-77da8b1&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7494)<ec><v0>:35874   |GFXD-DD-DISKSTORE               |da99f2a1-56fa-47a6-a31b-ea12e8a&|/build-artifacts/scala-2.11/snappy&
127.0.0.1(7494)<ec><v0>:35874   |GFXD-DEFAULT-DISKSTORE          |5412d64a-dfd2-46dc-b3ba-09c2079&|/build-artifacts/scala-2.11/snappy&

12 rows selected
```
