## Offline Members: Manual Catch-Up to an Online Backup

If you must have a member offline during an online backup, you can manually back up its disk stores. Do one of the following:

-   Keep the member’s backup and restore separated, doing offline manual backup and offline manual restore, if needed.

-   Bring this member’s files into the online backup framework manually and create a restore script by hand, from a copy of another member’s script:

 1. Duplicate the directory structure of a backed up member for this member.

 2. Rename directories as needed to reflect this member’s particular backup, including disk store names.

 3. Clear out all files but the restore script.

 4. Copy in this member’s files.

 5. Modify the restore script to work for this member.
