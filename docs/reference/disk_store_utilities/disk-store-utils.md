---
title: Disk Store Recovery Utilities
---

The disk store data recovery utilities are provided to help extract available data from corrupted diks store files.

See <a href="../../disk_storage/extractor_overview.html#disk_storage" class="xref" title="In cases where disk store files become corrupted, or where you cannot restore disk store backups to members, RowStore provides a data extractor utility that attempts to recover as much data as possible from available disk store files. The recovered data is stored in multiple comma-separated values (CSV) files, which you can use to load the data into a new RowStore system.">Recovering Data from Disk Stores</a> for more instructions and examples of using the utilities.

-   **[dataextractor](../../reference/disk-store-utilities/dataextractor.html)**
    Operates against a set of available RowStore operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system.
-   **[dataextractloader](../../reference/disk-store-utilities/dataextractloader.html)**
    Takes the SQL, CSV, and recommendations output files from `dataextractor`, and uses those inputs to load recovered disk store data into a new RowStore system


