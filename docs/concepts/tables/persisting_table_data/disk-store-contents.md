# What SnappyData Writes to the Disk Store

For each disk store, SnappyData stores detailed information about related members and tables.

SnappyData stores these items in each disk store:

-   List of members that host the store and information on their status, such as running or offline and time stamps.

-   List of tables that use the disk store.

For each table in the disk store, SnappyData stores:

-   Configuration attributes pertaining to loading and capacity management, used to load the data quickly on startup.

-   Table DML operations.


