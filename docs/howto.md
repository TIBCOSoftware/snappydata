# Overview

This section introduces you to several common operations such as starting a cluster, working with tables (load, query, update), working with streams and running approximate queries.

**Running the Examples:**
Topics in this section refer to source code examples that are shipped with the product. Instructions to run these examples can be found in the source code.

Source code for these examples is located in the **quickstart/src/main/scala/org/apache/spark/examples/snappydata** and in **quickstart/python** directories of the TIBCO ComputeDB product distribution. 

You can run the examples in any of the following ways:

* **In the Local Mode**: By using the `bin/run-example` script (to run Scala examples) or by using the `bin/spark-submit` script (to run Python examples). These examples run colocated with Spark + Snappy Store in the same JVM. 

* **As a Job**:	Many of the Scala examples are also implemented as a Snappy job. In this case, examples can be submitted as a job to a running TIBCO ComputeDB cluster. Refer to the [jobs](howto/run_spark_job_inside_cluster.md) section for details on how to run a job.

!!! Note
	TIBCO ComputeDB also supports Java API. Refer to the [documentation](./programming_guide/building_snappydata_applications_using_spark_api.md) for more details on Java API.

The following topics are covered in this section:

* [How to Start a TIBCO ComputeDB Cluster](howto/start_snappy_cluster.md)<a id="howto-startcluster"></a>

* [How to Check the Status of a TIBCO ComputeDB Cluster](howto/check_status_cluster.md)<a id="howto-statuscluster"></a>

* [How to Stop a TIBCO ComputeDB Cluster](howto/stop_snappy_cluster.md)<a id="howto-stopcluster"></a>

* [How to Run a Spark Job inside the Cluster](howto/run_spark_job_inside_cluster.md)<a id="howto-job"></a>

* [How to Access TIBCO ComputeDB Store from an existing Spark Installation using Smart Connector](howto/spark_installation_using_smart_connector.md)<a id="howto-splitmode"></a>

* [How to Use Snappy Shell (snappy-sql)](howto/use_snappy_shell.md)

* [How to Create Row Tables and Run Queries](howto/create_row_tables_and_run_queries.md)<a id="howto-row"></a>

* [How to Create Column Tables and Run Queries](howto/create_column_tables_and_run_queries.md)<a id="howto-column"></a>

* [How to Load Data into TIBCO ComputeDB Tables](howto/load_data_into_snappydata_tables.md)<a id="howto-load"></a>

* [How to Load Data from External Data Stores (e.g. HDFS, Cassandra, Hive, etc)](howto/load_data_from_external_data_stores.md)<a id="howto-external-source"></a>

* [How to Perform a Colocated Join](howto/perform_a_colocated_join.md)<a id="howto-colacatedJoin"></a>

* [How to Connect using JDBC Driver](howto/connect_using_jdbc_driver.md)<a id="howto-jdbc"></a>

* [How to Store and Query JSON Objects](howto/store_and_query_json_objects.md)<a id="howto-JSON"></a>

* [How to Store and Query Objects](howto/store_and_query_objects.md)<a id="howto-objects"></a>

* [How to use Stream Processing with TIBCO ComputeDB](howto/use_stream_processing_with_snappydata.md)<a id="howto-streams"></a>

* [How to use Transactions Isolation Levels](howto/use_transactions_isolation_levels.md)<a id="howto-transactions"></a>

* [How to use Synopsis Data Engine to Run Approximate Queries](howto/use_synopsis_data_engine_to_run_approximate_queries.md)<a id="howto-sde"></a>

* [How to use Python to Create Tables and Run Queries](howto/use_python_to_create_tables_and_run_queries.md)<a id="howto-python"></a>

* [How to connect using ODBC Driver](howto/connect_using_odbc_driver.md)<a id="howto-odbc"></a>

* [How to connect to the Cluster from External Clients](howto/connect_to_the_cluster_from_external_clients.md)<a id="howto-external-client"></a><a id="howto-connect-externalclients"></a>

* [How to import data from a Hive Table into a TIBCO ComputeDB Table](howto/import_from_hive_table.md)<a id="howto-import-hive"></a>

* [How to Export and Restore table data to HDFS](howto/export_hdfs.md)<a id="howto-export-hdfs"></a>

* [How to Access TIBCO ComputeDB from Various SQL Client Tools](howto/connect_oss_vis_client_tools.md)

* [How to Connect TIBCO Spotfire® Desktop to TIBCO ComputeDB](howto/connecttibcospotfire.md)
* [How to Connect TIBCO® Data Virtualization to TIBCO ComputeDB](/howto/connecttibcodv.md)

* [How to Connect Tableau to TIBCO ComputeDB](howto/tableauconnect.md) 

* [How to use Apache Zeppelin with TIBCO ComputeDB](howto/use_apache_zeppelin_with_snappydata.md)<a id="howto-zeppelin"></a>

* [How to Configure Apache Zeppelin to Securely and Concurrently access the TIBCO ComputeDB Cluster](howto/concurrent_apache_zeppelin_access_to_secure_snappydata.md)<a id="howto-concurrentzeppelin"></a>

