# SYSDISKSTOREIDS

Contains information about all disk stores IDs created in the SnappyData distributed system.

| Column Name          | Type    | Length | Nullable | Contents|
|----------------------|---------|--------|----------|--------------------------------------------------------------------------------------------------------------------------------------|
|MEMBERID | VARCHAR|128 |false |The ID of the cluster member.|
|NAME| VARCHAR|128| false|The diskstore name. Two inbuilt diskstores one for DataDictionary is named `GFXD-DD-DISKSTORE` while the default data diskstore is named `GFXD-DEFAULT-DISKSTORE`.|
|ID|CHAR|36| false|The unique diskstore ID. This is what appears in log files or output of `snappy-status-all.sh` script if a member is waiting for another node to start to sync its data (if it determines the other node may have more recent data). |
|DIRS| VARCHAR|32672 |false |Comma-separated list of directories used for the diskstore. These are the ones provided in [CREATE DISKSTORE](../sql_reference/create-diskstore.md) or else if no explicit directory was provided, then the working directory of the node. |

For example:

```
snappy> create diskstore d1 ('D1');
snappy> select * from sys.diskstoreids;


MEMBERID                        |NAME                    |ID                     |DIRS
----------------------------------------------------------------------------------------------------------------------
192.168.1.137(27269)<v3>:2472   |GFXD-DEFAULT-DISKSTORE  |0206fd8a-c4db-4d3d-9696-2d1c0826ed62|/server1
192.168.1.137(27269)<v3>:2472   |D1                      |20fa202e-035a-4ab9-995d-5e6a2e514dba|/server1/D1
192.168.1.137(27269)<v3>:2472   |GFXD-DD-DISKSTORE       |65af3ef8-54ac-4b8f-b280-2190673855d8|/server1/datadictionary
192.168.1.137(27095)<v2>:45630  |GFXD-DEFAULT-DISKSTORE  |b042dfd9-3f48-4357-a5c8-04e0f36c3ebf|/server2
192.168.1.137(27095)<v2>:45630  |D1                      |edb7a028-6730-4974-a465-63b2d5b20eda|/server2/D1
192.168.1.137(27095)<v2>:45630  |GFXD-DD-DISKSTORE       |3fc69a16-0531-4290-ab9b-141ae95c19fc|/server2/datadictionary
localhost(26614)<v0>:6685       |GFXD-DEFAULT-DISKSTORE  |ce5a4953-d0a7-4713-a3a6-ba68f85d1351|/locator
localhost(26614)<v0>:6685       |GFXD-DD-DISKSTORE       |e8cff498-f408-4e71-8b52-9de07e77a7a7|/locator/datadictionary
192.168.1.137(26921)<v1>:35276  |GFXD-DD-DISKSTORE       |f28c1f95-eac1-443b-9278-1c7d0c396a80|/server3/datadictionary
192.168.1.137(26921)<v1>:35276  |D1                      |70770c4b-8e77-46fc-a9ea-91115c9e0cfb|/server3/D1
192.168.1.137(26921)<v1>:35276  |GFXD-DEFAULT-DISKSTORE  |5bb7f76b-27d3-4431-830e-29f49ae61df4|/server3
192.168.1.137(27464)<v4>:65057  |GFXD-DEFAULT-DISKSTORE  |68ff3122-13cd-43cc-b1e4-7a64079cf50c|/lead
```

