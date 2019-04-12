# Design Principles of Scalable, Partition-Aware Databases
<a id="design-schema"></a>

The general strategy for designing a TIBCO ComputeDB™ database is to identify the tables to partition or replicate in the TIBCO ComputeDB cluster and then to determine the correct partitioning key(s) for partitioned tables. Usually, this requires an iterative process to produce the optimal design:

1. Read [Identify Entity Groups and Partitioning Keys](#entity_groups) and [Replicate Dimension Tables](optimizing_query_latency.md#partition-replicate) to understand the basic rules for defining partitioned or replicated tables.

2. Evaluate your data access patterns to define those entity groups that are candidates for partitioning. Focus your efforts on commonly-joined entities. Colocating commonly joined tables improve the performance of join queries by avoiding shuffle of data when the join is on partitioning keys.

3. Identify all of the tables in the entity groups.

4. Identify the “partitioning key” for each partitioned table. The partitioning key is the column or set of columns that are common across a set of related tables. Look for parent-child relationships in the joined tables.

5. Identify all of the tables that are candidates for replication. You can replicate table data for high availability, or for colocating table data that is necessary to execute joins.

<a id="entity_groups"></a>
## Identify Entity Groups and Partitioning Keys

In relational database terms, an entity group corresponds to rows that are related to one another through foreign key relationships. Members of an entity group are typically related by parent-child relationships and can be managed in a single partition. To design a TIBCO ComputeDB database for data partitioning, begin by identifying “entity groups” and their associated partitioning keys.

For example:

* In a customer order management system, most transactions operate on data related to a single customer at a time. Queries frequently join a customer’s billing information with their orders and shipping information. For this type of application, you partition related tables using customer identity. Any customer row along with their “order” and “shipping” rows forms a single entity group that has the customer ID as the entity group identity (the partitioning key). You would partition related tables using the customer identity, which would enable you to scale the system linearly by adding more members to support additional customers.

* In a system that manages a product catalog (product categories, product specifications, customer reviews, rebates, related products, and so forth) most data access focuses on a single product at a time. In this type of system, you would partition data on the product key and add members as needed to manage additional products.

* In an online auction application, you may need to stream incoming auction bids to hundreds of clients with very low latency. To do so, you would manage selected “hot” auctions on a single partition, so that they receive sufficient processing power. As the processing demand increases, you would add more partitions and route the application logic that matches bids to clients to the data store itself.

* In a financial trading engine that constantly matches bid prices to asking prices for thousands of securities, you would partition data using the ID of the security. To ensure low-latency execution when a security’s market data changes, you would colocate all of the related reference data with the matching algorithm.

  
By identifying entity groups and using those groups as the basis for TIBCO ComputeDB partitioning and colocation, you can realize these benefits:

* **Rebalancing**: TIBCO ComputeDB rebalances the data automatically by making sure that related rows are migrated together and without any integrity loss. This enables you to add capacity as needed.

* **Parallel scatter-gather**: Queries that cannot be pruned to a single partition are automatically executed in parallel on data stores. 

* **Sub-queries on remote partitions**: Even when a query is pruned to a single partition, the query can execute sub-queries that operate on data that is stored on remote partitions.

<a id="adapting_existing_schema"></a>
## Adapting a Database Schema

If you have an existing database design that you want to deploy to TIBCO ComputeDB, translate the entity-relationship model into a physical design that is optimized for TIBCO ComputeDB design principles.

## Guidelines for Adapting a Database to TIBCO ComputeDB
This example shows tables from the Microsoft [Northwind Traders sample database](https://www.microsoft.com/en-us/download/details.aspx?id=23654).

To adapt this schema for use in TIBCO ComputeDB, follow the basic steps outlined in [Design Principles of Scalable, Partition-Aware Databases](#design-schema):

1. Determine the entity groups.

	Entity groups are generally coarse-grained entities that have children, grandchildren, and so forth, and they are commonly used in queries. This example chooses these entity groups:

    | Entity Group | Description |
    |--------|--------|
    |Customer|This group uses customer identity along with orders and order details as the children|
    |Product|This group uses product details along with the associated supplier information|

2. Identify the tables in each entity group.
	Identify the tables that belong to each entity group. In this example, entity groups use the following tables.

    | Entity Group |Tables |
    |--------|--------|
    |Customer|Customers </br>Orders</br>Shippers</br>Order Details|
    |Product|Product</br>Suppliers</br>Category|

3. Define the partitioning key for each group.</br>
	In this example, the partitioning keys are:

    | Entity Group |Partitioning key |
    |--------|--------|
    |Customer|CustomerID|
    |Product|ProductID|

    This example uses customerID as the partitioning key for the Customer group. The customer row and all associated orders are colocated into a single partition. To explicitly colocate Orders with its parent customer row, use the `colocate with` clause in the create table statement:
		
		create table orders (<column definitions>) 
		using column options ('partition_by' customerID, 
		'colocate_with' customers);

    In this way, TIBCO ComputeDB supports any queries that join any the Customers and Orders tables. This join query would be distributed to all partitions and executed in parallel, with the results streamed back to the client:

	    select * from customer c , orders o where c.customerID = o.customerID;

    A query such as this would be pruned to the single partition that stores “customer100” and executed only on that TIBCO ComputeDB member:

	    select * from customer c, orders o where c.customerID = o.customerID  and c.customerID = 'customer100';

    The optimization provided when queries are highly selective comes from engaging the query processor and indexing on a single member rather than on all partitions. With all customer data managed in memory, query response times are high-speed. </br>
    Finally, consider a case where an application needs to access customer order data for several customers:

        select * from customer c, orders o 
        where c.customerID = o.customerID and c.customerID IN ('cust1', 'cust2', 'cust3');
    
    Here, TIBCO ComputeDB prunes the query execution to only those partitions that host ‘cust1’, 'cust2’, and 'cust3’. The union of the results is then returned to the caller.
    Note that the selection of customerID as the partitioning key means that the OrderDetails and Shippers tables cannot be partitioned and colocated with Customers and Orders (because OrderDetails and Shippers do not contain the customerID value for partitioning). 

4. Identify replicated tables.</br>
	If we assume that the number of categories and suppliers rarely changes, those tables can be replicated in the TIBCO ComputeDB cluster (replicated to all of the TIBCO ComputeDB members that host the entity group). If we assume that the Products table does change often and can be relatively large, then partitioning is a better strategy for that table.
	So for the product entity group, table Products is partitioned by ProductID, and the Suppliers and Categories tables are replicated to all of the members where Products is partitioned.
	Applications can now join Products, Suppliers and categories. For example:

        select * from Products p , Suppliers s, Categories c 
		where c.categoryID = p.categoryID and p.supplierID = s.supplierID 
		and p.productID IN ('someProductKey1', ' someProductKey2', ' someProductKey3');
    

	In the above query, TIBCO ComputeDB prunes the query execution to only those partitions that host 'someProductKey1’, ’someProductKey2’, and ’someProductKey3.’
