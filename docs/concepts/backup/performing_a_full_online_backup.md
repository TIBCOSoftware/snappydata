## Performing a Full Online Backup

1.  Stop all transactions running in your distributed system, and do not execute DML statements during the backup. SnappyData does not support backing up a disk store while live transactions are taking place or when concurrent DML statements are being executed.

2.  Run the `snappy backup` command, providing your backup directory location. For example:

    ``` 
    $ snappy backup /export/fileServerDirectory/gemfireXDBackupLocation
         -locators=machine.name[26340]
    ```

3.  Read the message that reports on the success of the operation.

    If the operation is successful, you see a message like this:

    ``` 
    Connecting to distributed system: locators=machine.name[26340]
    The following disk stores were backed up:
        DiskStore at hosta.machine.name /home/user/dir1
        DiskStore at hostb.machine.name /home/user/dir2
    Backup successful.
    ```

    If the operation does not succeed at backing up all known members, you see a message like this:

    ``` 
    Connecting to distributed system: locators=machine.name[26357]
    The following disk stores were backed up:
        DiskStore at hosta.machine.name /home/user/dir1
        DiskStore at hostb.machine.name /home/user/dir2
    The backup may be incomplete. The following disk stores are not online:
        DiskStore at hostc.machine.name /home/user/dir3
    ```

    A member that fails to complete its backup is noted in this ending status message and leaves the file `INCOMPLETE_BACKUP` in its highest level backup directory. Offline members leave nothing, so you only have this message from the backup operation itself. Although offline members cannot back up their disk stores, a complete backup can be obtained if at least one copy of the data is available in a running member.
