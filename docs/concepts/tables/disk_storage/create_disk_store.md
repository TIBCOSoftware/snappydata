# Creating a Disk Store or Using the Default

You can create a disk store for persistence and/or overflow or use the default disk store. Data from multiple tables can be stored in the same disk store.

<a id="how_disk_stores_work__section_5C032D3B2A534DCFA72F436E4A644B4F"></a>
## Default Disk Stores


<a id="how_disk_stores_work__section_1A93EFBE3E514918833592C17CFC4C40"></a>

Tables that do not name a disk store but specify persistence or overflow in their `CREATE TABLE` statement are automatically assigned to the default disk store, <span class="ph filepath">GFXD-DEFAULT-DISKSTORE</span>. Also, gateway, AsyncEventListener, and DBSynchronizer queues always use the default disk store. The default diskstore is saved to the SnappyData data store's working directory, unless you change the value of the `sys-disk-dir` boot property to specify another location.

!!!Note
	SnappyData locator and data store members also create disk store files in order to persist the data dictionary for the tables and indexes created the SnappyData distributed system. These persistence files are stored in the <span class="ph filepath">datadictionary</span> subdirectory of each locator and data store that joins the distributed system. The data dictionary is always persisted, regardless of whether you configure data persistence or overflow for individual tables. Table data is not persisted by default; if you shut down all SnappyData members, the tables are empty on the next startup.</br> Never move or modify the <span class="ph filepath">datadictionary</span> subdirectory or any other disk store directory. If the data dictionary or a disk store directory of a SnappyData locator or data store member is unavailable, other members may fail to start if the "offline" member potentially holds a more recent copy of the data. In this case, members will display a `ConflictingPersistentDataException` when attempting to start.

</p>

<a id="how_disk_stores_work__section_442810E05CB8492A898FB583B7E82C0C"></a>

## Creating a Named Disk Store

You create a named disk store in the data dictionary using the [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) DDL statement. You then assign the disk store to an individual table by specifying the disk store in the table's [CREATE TABLE](../../../reference/sql_reference/create-table.md) DDL statement. You can store data from multiple tables and queues in the same named disk store.
