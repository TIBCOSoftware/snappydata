# Disk Store Recovery Utilities

The disk store data recovery utilities are provided to help extract available data from corrupted diks store files.

See [Recovering Data from Disk Stores](../../../concepts/tables/disk_storage/recovering-data-from-disk-stores.md) for more instructions and examples of using the utilities.

-   **[dataextractor](dataextractor.md)**
    Operates against a set of available RowStore operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system.

-   **[dataextractloader](dataextractloader.md)**
    Takes the SQL, CSV, and recommendations output files from `dataextractor`, and uses those inputs to load recovered disk store data into a new RowStore system


