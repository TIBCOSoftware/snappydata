# Disk Store File Names and Extensions

Disk store files include store management and access control files and the operation log, or oplog, files, consisting of one file for deletions and another for all other operations.

<a id="file_names_and_extensions__section_AE90870A7BDB425B93111D1A6E166874"></a>
The next table describe file names and extensions; they are followed by example disk store files.

<a id="file_names_and_extensions__section_C99ABFDB1AEA4FE4B38F5D4F1D612F71"></a>

## File Names

File names have three parts.

**First Part of File Name: Usage Identifier**

| Usage Identifier Values | Usage                                                                 | Examples                                                  |
|-------------------------|-----------------------------------------------------------------------|-----------------------------------------------------------|
| OVERFLOW                | Oplog data from overflow tables and queues only.                      | OVERFLOWoverflowDS1\_1.crf                                |
| BACKUP                  | Oplog data from persistent and persistent+overflow tables and queues. | BACKUPoverflowDS1.if, BACKUPGFXD-DEFAULT-DISKSTORE.if     |
| DRLK\_IF                | Access control - locking the disk store.                              | DRLK\_IFoverflowDS1.lk, DRLK\_IFGFXD-DEFAULT-DISKSTORE.lk |



**Second Part of File Name: Disk Store Name**

| Disk Store Name Values  | Usage                                                                                                                    | Examples                                                                                                                                                                                                                                                                                        |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| &lt;disk store name&gt; | Non-default disk stores.                                                                                                 | name="OVERFLOWDS1" DRLK\_IFOVERFLOWDS1.lk, name="PERSISTDS1" BACKUPPERSISTDS1\_1.crf </br>**Note**: SnappyData internally converts and uses disk store names in upper case characters, even if you specify lower case characters in the DDL statement. </p> |
| GFXD-DEFAULT-DISKSTORE  | Default disk store name, used when persistence or overflow are specified on a table or queue but no disk store is named. | DRLK\_IFGFXD-DEFAULT-DISKSTORE.lk, BACKUPGFXD-DEFAULT-DISKSTORE\_1.crf                                                                                                                                                                                                                          |
| GFXD-DD-DISKSTORE       | Default disk store for persisting the data dictionary.                                                                   | BACKUPGFXD-DD-DISKSTORE\_1.crf                                                                                                                                                                                                                                                                  |


**Third Part of File Name: oplog Sequence Number**

| oplog Sequence Number             | Usage                                           | Examples                                                                     |
|-----------------------------------|-------------------------------------------------|------------------------------------------------------------------------------|
| Sequence number in the format \_n | Oplog data files only. Numbering starts with 1. | OVERFLOWoverflowDS1\_1.crf, BACKUPpersistDS1\_2.crf, BACKUPpersistDS1\_3.crf |

<a id="file_names_and_extensions__section_4FC89D10D6304088882B2E278A889A9B"></a>

## File Extensions

| File Extension Values | Usage                                            | Notes                                                                                                                                                                                                                                                                                          |
|-----------------------|--------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| if                    | Disk store metadata                              | Stored in the first disk-dir listed for the store. Negligible size - not considered in size control.                                                                                                                                                                                           |
| lk                    | Disk store access control                        | Stored in the first disk-dir listed for the store. Negligible size - not considered in size control.                                                                                                                                                                                           |
| crf                   | Oplog: create, update, and invalidate operations | Pre-allocated 90% of the total max-oplog-size at creation.                                                                                                                                                                                                                                     |
| drf                   | Oplog: delete operations                         | Pre-allocated 10% of the total max-oplog-size at creation.                                                                                                                                                                                                                                     |
| krf                   | Oplog: live key and crf offset information       | Created after the oplog has reached the max-oplog-size, before rolling over to a new file. Used to improve performance at startup.                                                                                                                                                             |
| irf                   | Oplog: index recovery file                       | Like the krf file, this file is created after the oplog has reached the max-oplog-size, before rolling over to a new file. This file contains the indexed column values that correspond to all of the live keys in the krf file. Used to improve the performance of index recovery at startup. |


