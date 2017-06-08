# Using Column vs Row Table

A columnar table stores the data in column by column whereas a row table stores data row by row.

**When to use column tables:**
A column table has distinct advantages for OLAP kind of scenarios and are recommended to be used for large tables used in OLAP scenarios. These table are rarely mutated (deleted/updated).  
For a given query on a column table, only required columns are needed to be read, thus, giving a better scan performance.  Thus aggregation queries run very fast on a column table compared  to a row table (since only required subset columns are to be scanned).

Another advantage that the column table offers is compression of data which reduces the total storage footprint for large tables. 

Column tables are not suitable for OLTP scenarios. In this case row tables are recommended.

**When to use row tables:**
Row tables are designed to return the entire row efficiently and are are suited for OLTP scenarios when the tables are required to be mutated frequently (when the table rows need to be updated/deleted based on some conditions). In such cases, row tables offer distinct advantages over the column tables. 
Row tables are also suitable for point queries (for example, queries that select only a few records based on certain where clause conditions). Row tables are also suitable to create small dimension tables as these can be created as replicated tables (table data replicated on all data servers).
Row tables also allow creation of index on certain columns of the table to improve the performance.

!!! Note
	In the current release of SnappyData, updates and deletes are not supported on column tables. This feature will be added in a future release.

# Using Partitioned vs Replicated Table
In SnappyData, row tables can be either partitioned across all servers or replicated on every server. Column tables are always partitioned. For row tables, large fact tables should be partitioned whereas dimension tables can be replicated.

The SnappyData architecture encourages you to denormalize “dimension” tables into fact tables when possible, and then replicate remaining dimension tables to all datastores in the distributed system.

Most databases follow the [star schema](http://en.wikipedia.org/wiki/Star_schema) design pattern where large “fact” tables store key information about the events in a system or a business process. For example, a fact table would store rows for events like product sales or bank transactions. Each fact table generally has foreign key relationships to multiple “dimension” tables, which describe further aspects of each row in the fact table.

When designing a database schema for SnappyData, the main goal with a typical star schema database is to partition the entities in fact tables. Slow-changing dimension tables should then be replicated on each data store that hosts a partitioned fact table. In this way, a join between the fact table and any number of its dimension tables can be executed concurrently on each partition, without requiring multiple network hops to other members in the distributed system.

# Applying a Partitioning Scheme

Collocating frequently joined partitioned tables is the best practice to improve performance of join queries. When two tables are partitioned on columns and collocated, it forces partitions having the same values for those columns in both tables to be located on the same SnappyData member. So in a join query, the join operation is performed on each node's  local data. 
If the two tables are not collocated, partitions with same column values for the two tables could be on different nodes thus requiring the data to be shuffled between nodes causing the query performance to degrade.

For an example on collocated joins, refer to [How-to link know how to collocate tables for doing a collocated join](howto.md/#how-to-perform-a-collocated-join).

# Using Redundancy

REDUNDANCY clause of CREATE TABLE specifies the number of secondary copies you want to maintain for your partitioned table. This allows the table data to be highly available even if one of the SnappyData members fails or shuts down. 
A REDUNDANCY value of 1 is recommended to maintain a secondary copy of the table data. A large number of redundant copies has an adverse impact on performance, network usage, and memory usage.

For an example on using the REDUNDANCY clause refer to [Tables in SnappyData](programming_guide.md/#tables-in-snappydata).

-------------

# Database design (schema definition):

## How to decide row table/vs column table (row partitioned/replicated)

## Partitioning scheme to use on column and row table/Collocated joins
(https://jira.snappydata.io/browse/CRQ-23)

## Redundancy - when to use

## Row table/column table/replicated tables

## Overflow configuration


<mark> Jags Comment</mark>
- - How to do physical design with SnappyData? Start with the "designing rowstore dbs" in our current rowstore docs. Needs a lot of refinement. A lot of the principles with colocation still apply.

- Using Row table or column table - When to use what?
Partitioning/Redundancy/Persistence

- Design Principles of Scalable, 

- Identify Entity Groups and Partitioning Keys 

- Replicate Dimension Tables 

- Example: Adapting a Database Schema for SnappyData

- Dealing with Many-to-Many Relationships 
http://rowstore.docs.snappydata.io/docs/data_management/database_design_chapter.html