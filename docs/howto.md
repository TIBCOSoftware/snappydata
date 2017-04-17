#Overview
This section introduces you to several common operations such as, starting a cluster, working with tables(load, query, update), working with streams and running approximate queries.

**Running the Examples:**
Topics in this section refer to source code examples that are shipped with the product. Instructions to run these examples can be found in the source code.

Source code for these examples is located in the **quickstart/src/main/scala/org/apache/spark/examples/snappydata** and in **quickstart/python** directories of the SnappyData product distribution.

Refer to the [Getting Started](quickstart.md) to either run SnappyData on premise, using AWS or Docker. 

You can run the examples in any of the following ways:

* **In the Local Mode**: By using `bin/run-example` script (to run Scala examples) or by using `bin/spark-submit` script (to run Python examples). The examples run collocated with Spark+SnappyData Store in the same JVM. 

* **As a Job**:	Many of the Scala examples are also implemented as a SnappyData job. In this case, examples can be submitted as a job to a running SnappyData cluster. Refer to [jobs](#howto-job) section for details on how to run a job.

<Note> Note: SnappyData also supports Java API. Refer to the [documentation](programming_guide/#building-snappy-applications-using-spark-api) for more details on Java API.</note>

The following topics are covered in this section:

* [How to Start a SnappyData Cluster](how_to/start_snappydata_cluster.md#howto-startCluster)

* [How to Run Spark Job inside the Cluster](how_to/run_spark_job_inside_the_cluster.md#howto-job)

* [How to Access SnappyData Store from existing Spark Installation using Smart Connector](how_to/access_snappydata_store.md#howto-splitmode)

* [How to Create Row Tables and Run Queries](how_to/create_column_tables_and_run_queries.md#howto-row)

* [How to Create Column Tables and Run Queries](how_to/create_row_tables_and_run_queries.md#howto-column)

* [How to Load Data in SnappyData Tables](how_to/load_data_in_snappydata_tables.md#howto-load)

* [How to perform a Collocated Join](how_to/perform_a_collocated_join.md#howto-collacatedJoin)

* [How to Connect using JDBC Driver](how_to/connect_using_jdbc_driver.md#howto-jdbc)

* [How to Store and Query JSON Objects](how_to/store_and_query_json_objects.md#howto-JSON)

* [How to Store and Query Objects](how_to/store_and_query_objects.md#howto-objects)

* [How to Use Stream Processing with SnappyData](how_to/use_stream_processing.md#howto-streams)

* [How to Use Synopsis Data Engine to Run Approximate Queries](how_to/use_synopsis_data_engine.md#howto-sde)

* [How to Use Python to Create Tables and Run Queries](how_to/use_python_to_create_tables_and_run_queries.md#howto-python)

<mark>

#EXTRA "HOW TO" TOPICS FOR GA REALEASE

* How to connect to SnappyData using third party tools-
	- Tablaeu: Steps for how to use with SnappyData
	- Zeppelin: Steps for how to use with SnappyData (already covered in iSight?)

</mark>
