<a id="partition-replicate"></a>
## Using Partitioned vs Replicated Row Table

In SnappyData, row tables can be either partitioned across all servers or replicated on every server. For row tables, large fact tables should be partitioned whereas, dimension tables can be replicated.

The SnappyData architecture encourages you to denormalize “dimension” tables into fact tables when possible, and then replicate remaining dimension tables to all datastores in the distributed system.

Most databases follow the [star schema](http://en.wikipedia.org/wiki/Star_schema) design pattern where large “fact” tables store key information about the events in a system or a business process. For example, a fact table would store rows for events like product sales or bank transactions. Each fact table generally has foreign key relationships to multiple “dimension” tables, which describe further aspects of each row in the fact table.

When designing a database schema for SnappyData, the main goal with a typical star schema database is to partition the entities in fact tables. Slow-changing dimension tables should then be replicated on each data store that hosts a partitioned fact table. In this way, a join between the fact table and any number of its dimension tables can be executed concurrently on each partition, without requiring multiple network hops to other members in the distributed system.

