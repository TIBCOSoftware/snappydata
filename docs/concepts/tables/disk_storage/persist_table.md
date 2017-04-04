# Persisting Table Data to a Disk Store

You configure the persistence settings for a partitioned or replicated table when you create the table with the CREATE TABLE DDL statement. SnappyData automatically recovers data from disk for persistent tables when you restart SnappyData members.

**Procedure**

1.  Ensure that the data dictionary is persisted in your SnappyData cluster. SnappyData persists the data dictionary by default for all data stores, but you can explicitly enable or disable data dictionary persistence using the `persist-dd` boot property. 

	!!! Note 
    	All SnappyData data stores in the same cluster must use a consistent `persist-dd` value. Accessors cannot persist data, and you cannot set `persist-dd` to true for an accessor. </p>

2.  Create the disk store that you want to use for persisting the table's data, or use the default disk store. See <a href="create_disk_store.html#how_disk_stores_work" class="xref" title="You can create a disk store for persistence and/or overflow or use the default disk store. Data from multiple tables can be stored in the same disk store.">Creating a Disk Store or Using the Default</a>.

3.  Specify table persistence and the named disk store in the CREATE TABLE statement. For example:
    
        CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT ) 
             persistent 'OrdersDiskStore' asynchronous


    This example uses asynchronous writes to persist table data to the "OrdersDiskStore."

    !!! Note 
    	Persistent tables must be associated with a disk store. If you do not specify a named disk store in the CREATE TABLE statement, SnappyData persists the table to the default disk store. For example, the following statement persists the new table to the default disk store:

        CREATE TABLE Orders(OrderId INT NOT NULL,ItemId INT ) 
             persistent asynchronous

4.  When starting SnappyData loctors and data stores, enable network partition to help guard against inconsistent data being written to disk store files. For example:

	    $ snappy locator start -enable-network-partition-detection=true

    If you start a member that maintains persistent data but you do not enable network partition detection, the member logs the following warning:
    
        Creating persistent region region-name, but enable-network-partition-detection is set to false. Running with network partition 
        detection disabled can lead to an unrecoverable system in the event of a network split.
    
    See <mark>Detecting and Handling Network Segmentation (&quot;Split Brain&quot;) TO BE CONFIRMED </mark>for more information. 


**See More:**

* [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md)

* [CREATE TABLE](../../../reference/sql_reference/create-table.md)

* [Evicting Table Data from Memory](../../../concepts/tables/evicting_table_data_from_memory.md)
