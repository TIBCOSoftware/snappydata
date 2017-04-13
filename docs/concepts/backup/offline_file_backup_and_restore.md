## Offline File Backup and Restore

To back up your offline system:

 1.  Validate, and consider compacting your disk stores before backing them up. See [Compacting Disk Store Log Files](../tables/persisting_table_data/compacting_disk_stores.md).

 2.  Copy all disk store files, and any other files you want to save, to your backup locations.

To restore a backup of an offline system:

 1.  Make sure the system is either down or not using the directories you will use for the restored files.

 2.  Make sure your members are configured to use the directories where you put the files.

 3.  Reverse your backup file copy procedure, copying all the backed up files into the directories you want to use.

 4.  Start the system members.