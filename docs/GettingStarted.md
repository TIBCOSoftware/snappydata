## Introduction

SnappyData fuses Apache Spark with an in-memory database to deliver a data engine capable of stream processing, transactions and interactive analytics in a single cluster.

Apache Spark is a general purpose parallel computational engine for analytics at scale. It's primary design center is batch and working with disparate data sources. While, this provides rich unification of data this is also in-efficient. Analytic processing requires massive data sets to be repeatedly copied, data reformatted to suit Spark and ultimately not meeting the interactive analytic performance promise. In fact, a number of the optimizations (vectorization, code gen) in its "Tungsten" engine become ineffective.

Instead, we take a very different approach. SnappyData fuses an low latency, highly available in-memory transactional database into Spark with shared memory management and optimizations. The net effect is an order of magnitude performance improvement even compared to native spark caching and more than 2 orders of magnitude performance benefit compared to working with external data sources like NoSQL. For more information see, 
the [SnappyData Approach](intro/about.md#approach)

##WHAT IS UNIQUE?
**True interactive analytics at scale**: The state of the art today does not offer practical solutions for ad-hoc interactive analytics over large data sets at high concurrency. Through smart colocation of data, vectorization and code generation we can speed up analytical processing by two orders of magnitude over even the fastest in-memory data platforms in the market today.

**Supports diverse audience needs**: App developers, data engineers and scientists and more. Unlike common high performance analytical engines that work with column oriented structured data (SQL) we support structured as well as semi-structured data really well (JSON, CSV, parquet, Objects). All high level APIs build on top of the core Spark DataFrame abstraction and hence all data is easily and readily shared across diverse applications without needing expensive and traditional ETL.

**Data Unification engine**: Data stored in in-memory tables, streams, HDFS, relational databases, CSV files, S3 folders, and NoSQL databases can be optimally parallel processed using using the SQL or Spark APIs.

**Data reduction (Synopses data for interactive analytics)**: Machine learning uses statistical algorithms to speed up training and prediction. We apply similar ideas to the core of our DB engine - Instead of working with very large data sets, we condense them by extracting the data distribution patterns in key dimensions to answer general purpose queries at Google-like speeds. Dealing with less data implies less complexity in cluster deployments and cost savings.


**Related Topics:**

* Challenges

* SnappyData Cluster Architecture

* SnappyData Core Components

* Key Benefits