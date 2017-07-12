# Adapting a Database Schema

If you have an existing database design that you want to deploy to SnappyData, translate the entity-relationship model into a physical design that is optimized for SnappyData design principles.

## Guidelines for Adapting a Database to SnappyData
This example shows tables from the Microsoft [Northwind Traders sample database](http://msdn.microsoft.com/en-us/library/aa276825(SQL.80).aspx).

In order to adapt this schema for use in SnappyData, follow the basic steps outlined in [Design Principles of Scalable, Partition-Aware Databases](partition_aware_database_design.md):

1. Determine the entity groups.

	Entity groups are generally course-grained entities that have children, grand children, and so forth, and they are commonly used in queries. This example chooses these entity groups:

    | Entity Group | Description |
    |--------|--------|
    |Customer|This group uses the customer identity along with orders and order details as the children|
    |Product|This group uses product d etails along with the associated supplier information|

2. Identify the tables in each entity group.
	Identify the tables that belong to each entity group. In this example, entity groups use the following tables.

    | Entity Group |Tables |
    |--------|--------|
    |Customer|Customers </br>Orders</br>Shippers</br>Order Details|
    |Product|Product</br>Suppliers</br>Category|

3. Define the partitioning key for each group.
	In this example, the partitioning keys are:

    | Entity Group |Partitioning key |
    |--------|--------|
    |Customer|CustomerID|
    |Product|ProductID|

    This example uses customerID as the partitioning key for the Customer group. The customer row and all associated orders will be colocated into a single partition. To explicitly colocate Orders with its parent customer row, use the `colocate with` clause in the create table statement:

		create table orders (<column definitions, constraints>)
		partition by (customerID)
		colocate with (customers);

    In this way, SnappyData supports any queries that join any the Customers and Orders tables. This join query would be distributed to all partitions and executed in parallel, with the results streamed back to the client:

	    select * from customer c , orders o where c.customerID = o.customerID;

    A query such as this would be pruned to the single partition that stores “customer100” and executed only on that SnappyData member:

	    select * from customer c, orders o where c.customerID = o.customerID  and c.customerID = 'customer100';


    The optimization provided when queries are highly selective comes from engaging the query processor and indexing on a single member rather than on all partitions. With all customer data managed in memory, query response times are very fast. Consider how the above query would execute if the primary key was not used to partition the table. In this case, the query would be routed to each partition member where an index lookup would be performed, even though only a single member might have any data associated with the query. </br>
    Finally, consider a case where an application needs to access customer order data for several customers:

        select * from customer c, orders o 
        where c.customerID = o.customerID and c.customerID IN ('cust1', 'cust2', 'cust3');
    
    Here, SnappyData prunes the query execution to only those partitions that host ‘cust1’, 'cust2’, and 'cust3’. The union of the results is then returned to the caller.
    Note that the selection of customerID as the partitioning key means that the OrderDetails and Shippers tables cannot be partitioned and colocated with Customers and Orders (because OrderDetails and Shippers do not contain the customerID value for partitioning). If joins are required between these tables, then you may choose to replicate OrderDetails and Shippers to the datastores that host Customers and Orders, as described in the next step.

4. Identify replicated tables.
	If we assume that the number of categories and suppliers rarely changes, those tables can be replicated in the SnappyData cluster (replicated to all of the SnappyData members that host the entity group). If we assume that the Products table does change often and can be relatively large in size, then partitioning is a better strategy for that table.
	So for the product entity group, table Products is partitioned by ProductID, and the Suppliers and Categories tables are replicated to all of the members where Products is partitioned.
	Applications can now join Products, Suppliers and categories. For example:

        select * from Products p , Suppliers s, Categories c 
		where c.categoryID = p.categoryID and p.supplierID = s.supplierID 
		and p.productID IN ('someProductKey1', ' someProductKey2', ' someProductKey3');
    

	In the above query, SnappyData prunes the query execution to only those partitions that host 'someProductKey1’, ’ someProductKey2’, and ’ someProductKey3.’
