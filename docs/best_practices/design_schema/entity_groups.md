# Identify Entity Groups and Partitioning Keys

In relational database terms, an entity group corresponds to rows that are related to one another through foreign key relationships. Members of an entity group are typically related by parent-child relationships and can be managed in a single partition. To design a SnappyData database for data partitioning, begin by identifying “entity groups” and their associated partitioning keys.

For example:

* In a customer order management system, most transactions operate on data related to a single customer at a time. Queries frequently join a customer’s billing information with their orders and shipping information. For this type of application, you partition related tables using the customer identity. Any customer row along with their “order” and “shipping” rows forms a single entity group that has the customer ID as the entity group identity (the partitioning key). You would partition related tables using the customer identity, which would enable you to scale the system linearly by adding more members to support additional customers.

* In a system that manages a product catalog (product categories, product specifications, customer reviews, rebates, related products, and so forth) most data access focuses on a single product at a time. In this type of system, you would partition data on the product key and add members as needed to manage additional products.

* In an online auction application, you may need to stream incoming auction bids to hundreds of clients with very low latency. To do so, you would manage selected “hot” auctions on a single partition, so that they receive sufficient processing power. As the processing demand increases, you would add more partitions and route the application logic that matches bids to clients to the data store itself.

* In a financial trading engine that constantly matches bid prices to asking prices for thousands of securities, you would partition data using the ID of the security. To ensure low-latency execution when a security’s market data changes, you would colocate all of the related reference data with the matching algorithm.

!!! Note:
	[Sharding](http://en.wikipedia.org/wiki/Shard_(database_architecture)) is a database partitioning strategy where groups of table rows are stored on separate database servers and potentially different instances of the same schema, creating groups of data that can be accessed independently. SnappyData does encourage applications to use sharding techniques.
    
By identifying identify entity groups and using those groups as the basis for SnappyData partitioning and colocation, you can realize these benefits:

* **Rebalancing**: SnappyData rebalances the data automatically by making sure that related rows are migrated together and without any integrity loss. This enables you to add capacity as needed.

* **Distributed transactions**: SnappyData transaction boundaries are not limited to a single partition. Atomicity and isolation guarantees are provided across the entire distributed system.

* **Parallel scatter-gather**: Queries that cannot be pruned to a single partition are automatically executed in parallel on data stores. Joins can be performed between tables with the restriction that the joined rows are in fact colocated.

* **Subqueries on remote partitions**: Even when a query is pruned to a single partition, the query can execute subqueries that operate on data that is stored on remote partitions.
