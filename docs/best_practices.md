# Best Practices for Deploying and Managing SnappyData
The following topics are covered in this section:

* [Using Index](best_practises/using_index.md)

* [Using Collocated joins](best_practises/collocated_joins.md)

* [Physical Designing with SnappyData](best_practises/physical_design.md)

* [Using Row vs Column Table](best_practises/use_row_column_table.md)

* [Right Affinity Mode to Use](best_practises/affinity_mode_to_use.md)

* [Capacity Planning](best_practises/capacity_planning.md)

* [Preventing Disk Full Errors](best_practises/prevent_disk_full_errors.md)

* [Design Database and Schema](best_practises/design_schema.md)

Loading data from external repositories like S3, CSV, HDFS, RDBMS and NoSQL
 (Should be a separate section if we have enough content ... )
  - how to load from S3, CSV, HDFS using SQL external tables
     -- How to deal with missing data or ignoring errors (CSV)
  - How to manage parallelism for loading
  - how to transform datatypes before loading (using SQL Cast, expressions)
  - How to transform and enrich while loading (Using spark program)
  - How to load an entire RDB (from a schema)? Example that reads all table names from catalog and then execute select from external JDBC source table on each table?
  - How to load data from NoSQL stores like cassandra (give Cassandra example using Spark-cassandra package)
  
  Snapshotting state from SnappyData to external repositories like S3,CSV/parquet, RDBMS and NoSQL
   - We recommend backing up state from SnappyData into a repository like HDFS or S3 in Parquet format. 
   - Repeat the How-tos from the above section
   
 Optimizing Query performance
   - Explaining 'Explain plan' 
   - 'Where is the time spent' using SnappyData Pulse
   - Tuning the costs associated with data shuffling and configuring /tmp 
   - Tuning memory and eviction settings
   - Tuning JVM heap settings and GC settings
   - Tuning Offheap settings
   - Tuning broadcast thresholds
   - Tips for optimizing query performance when using the Smart connector
     -- e.g. when to use JDBC connection to route entire query vs only applying filters and projections    
   
 How to plan for large data volumes (maybe the same as capacity planning)
   - Sumedh, Hemant, etc are working on this
   
 
