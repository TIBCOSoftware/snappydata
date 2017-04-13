## What an Incremental Online Backup Saves

An incremental backup saves the difference between the last backup and the current data. An incremental backup copies only operation logs that are not already present in the baseline directories for each member. For incremental backups, the restore script contains explicit references to operation logs in one or more previously-chained incremental backups. When the restore script is run from an incremental backup, it also restores the operation logs from previous incremental backups that are part of the backup chain.

If members are missing from the baseline directory because they were offline or did not exist at the time of the baseline backup, those members place full backups of all their files into the incremental backup directory.

!!! Note:
	SnappyData does not support taking incremental backups on systems with live transactions, or when concurrent DML statements are being executed.</p>
