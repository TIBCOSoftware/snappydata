## Performing an Incremental Backup

1.  Stop all transactions running in your distributed system, and do not execute DML statements during the backup. SnappyData does not support backing up a disk store while live transactions are taking place or when concurrent DML statements are being executed.
2.  Perform a full online backup to create a a baseline backup for the distributed system:

    ``` 
    $ snappy backup /export/fileServerDirectory/gemfireXDBackupLocation
         -locators=machine.name[26340]
    ```

3.  To perform an incremental backup, execute the `snappy backup` command but specify the baseline directory as well as your incremental backup directory (they can be the same directory). For example:

    ``` 
    $ snappy backup -baseline=/export/fileServerDirectory/gemfireXDBackupLocation /export/fileServerDirectory/gemfireXDBackupLocation 
      -locators=machine.name[26340] 
    ```

    The tool reports on the success of the operation. If the operation is successful, you see a message like this:

    ``` 
    The following disk stores were backed up:
        DiskStore at hosta.gemstone.com /home/user/dir1
        DiskStore at hostb.gemstone.com /home/user/dir2
    Backup successful.
    ```

    If the operation does not succeed at performing an incremental backup on all known members, you see a message like this:

    ``` 
    The following disk stores were backed up:
        DiskStore at hosta.gemstone.com /home/user/dir1
        DiskStore at hostb.gemstone.com /home/user/dir2
    The backup may be incomplete. The following disk stores are not online:
        DiskStore at hostc.gemstone.com /home/user/dir3
    ```

    A member that fails to complete its backup is noted in this ending status message and leaves the file `INCOMPLETE_BACKUP`. The next time you perform a backup operation a full backup will be performed.

4.  Make additional incremental backups as necessary by executing the same `snappy backup` command described in the previous step.