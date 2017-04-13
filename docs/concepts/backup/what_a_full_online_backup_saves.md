## What a Full Online Backup Saves

For each member with persistent data, the backup includes:

-   Disk store files for all stores containing persistent tables.

-   Configuration files from the member startup (<span class="ph filepath">gemfirexd.properties</span>). These configuration files are not automatically restored, to avoid interfering with any more recent configurations. In particular, if these are extracted from a master `jar` file, copying the separate files into your working area could override the files in the `jar`.

-   A restore script, written for the member’s operating system, that copies the files back to their original locations. For example, in Windows, the file is `restore.bat` and in Linux, it is `restore.sh`.