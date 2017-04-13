# Recovering Data from Disk Stores

In cases where disk store files become corrupted, or where you cannot restore disk store backups to members, SnappyData provides a data extractor utility that attempts to recover as much data as possible from available disk store files. The recovered data is stored in multiple comma-separated values (CSV) files, which you can use to load the data into a new SnappyData system.

!!!Note:
	Whenever possible, SnappyData recommends that you recover data using an online or offline disk store backup, using the techniques described in [Backing Up and Restoring Disk Stores](../../backup/backup_restore_disk_store.md). The data recovery utilities provide a "best effort" attempt to recover data in the following types of failure scenarios:

	-   Corruption of disk store file data at the hardware level, with no access to recent disk store backups.

	-   Corruption of disk store file data caused by disk full conditions. See [Preventing Disk Full Errors](../../../troubleshooting/prevent_disk_full_errors.md).

</p>
See [Limitations for Data Recovery](extractor_topics.md#id_vnc_ysw_d4) to understand possible data consistency problems that can occur when using the data recovery utilities to extract and load data from disk stores.

-   **[How the Data Recovery Utilities Work](extractor_topics.md#disk_storage)**
    The `dataextractor` utility operates against a set of available SnappyData operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system. A `dataextractloader` utility takes the CSV file output along with a recommendations file, and uses those inputs to load the recovered data into a new SnappyData system.

-   **[Limitations for Data Recovery](extractor_topics.md#id_vnc_ysw_d4)**
    The `dataextractor` utility provides only a "best effort" attempt to recover disk store data. Keep these limitations in mind when you use the utilities

-   **[Requirements](extractor_topics.md#topic_eks_mxw_d4)**
    The following procedures and resources are required in order to use the data recovery utilities.
	-   **[Procedure for Recovering Data from Disk Stores](extractor_topics.md#topic_ddt_gbx_d4)**
    Follow these steps to extract available data from available SnappyData disk store files.

	-   **[Procedure for Loading Recovered Data into a New System](extractor_topics.md#topic_o3x_vfc_24)**
    Follow these steps to load the SQL and CSV files that were recovered using `dataextractor` into a new SnappyData system.
