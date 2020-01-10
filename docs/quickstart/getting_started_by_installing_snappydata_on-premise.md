<a id="getting-started-by-installing-snappydata-on-premise"></a>
# Getting Started by Installing TIBCO ComputeDB On-Premise

1. On the [TIBCO eDelivery website](https://edelivery.tibco.com), search for **TIBCO ComputeDB** and go to the **Product Detail** page.
2. Click **Download** and then enter your credentials. 
3. In the Download page, select the version number and then select **Linux**.
4. Read and accept the **END USER LICENSE AGREEMENT**.
5. Choose an installation option and then click **Download**.
6. After **TIB_compute_1.1.1_linux.tar.gz** is downloaded, run the following commands:

        $ tar -xzf TIB_compute_1.1.1_linux.tar.gz
        $ cd TIB_compute_1.1.1_linux/
        # Create a directory for TIBCO ComputeDB artifacts
        $ mkdir quickstartdatadir
        $./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log
    
	It opens the Spark shell. All TIBCO ComputeDB metadata, as well as persistent data, is stored in the directory **quickstartdatadir**.</br>
	The spark-shell can now be used to work with TIBCO ComputeDB using [SQL](using_sql.md) and [Scala APIs](using_spark_scala_apis.md).
7. Follow instructions [here](/howto/use_apache_zeppelin_with_snappydata.md), to use the product from Apache Zeppelin. 