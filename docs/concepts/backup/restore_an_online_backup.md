## Restore an Online Backup

The restore script included in the online backup copies files back to their original locations. You can do this manually if you wish.

**Prerequisites**

-   Your members are offline and the system is down.

-   Restore all SnappyData members that were active in the original system. (For example, you cannot use disk store backups to create a new system that hosts fewer datastores than were in the original distributed system.)

**Procedure**

1.  Navigate to the backup subdirectory with the timestamp of the backup that you want to restore. For example, if you performed multiple incremental backups, navigate to the latest backup directory in order to restore your system to the last available backup.

2.  Read the restore scripts to see where they will place the files and make sure the destination locations are ready. The restore scripts refuse to copy over files with the same names.

3.  Run each restore script on the host where the backup originated.

    In Windows, the file is `restore.bat` and in Linux, it is `restore.sh`.

    The restore copies the files back to their original location.

4.  Repeat this procedure as necessary for other members of the distributed system.

5.  After all disk stores have been restored, restart all members of the original cluster.