# Troubleshooting Error Messages
Error messages provide information about problems that might occur when setting up the SnappyData cluster or when running queries. </br>You can use the following information to resolve such problems.

<!-- --------------------------------------------------------------------------- -->

The following topics are covered in this section:

* [Region {0} has potentially stale data. It is waiting for another member to recover the latest data.](#region0)

* [XCL54.T Query/DML/DDL '{0}' canceled due to low memory on member '{1}'. Try reducing the search space by adding more filter conditions to the where clause. query](#query-dml-dll)

* [{0} seconds have elapsed while waiting for reply from {1} on {2} whose current membership list is: [{3}]](#seconds-elapsed)

* [Region {0} bucket {1} has persistent data that is no longer online stored at these locations: {2}](#persistent-data)

* [ForcedDisconnectException Error: "No Data Store found in the distributed system for: {0}"](#no-data-store)
* [Node went down or data no longer available while iterating the results](#queryfailiterate)
* [SmartConnector catalog is not up to date. Please reconstruct the Dataset and retry the operation.](#smartconnectorcatalog)
* [Cannot parse config: String: 1: Expecting end of input or a comma, got ':'](#jobsubmitsnap)
* [java.lang.IllegalStateException: Detected both log4j-over-slf4j.jar AND bound slf4j-log4j12.jar on the class path, preempting StackOverflowError](#javalangillegal)

<a id="region0"></a>
<error> **Error Message:** </error> 
<error-text>
Region {0} has potentially stale data. It is waiting for another member to recover the latest data.
My persistent id:</br>
{1}</br>
Members with potentially new data:</br>
{2}Use the "{3} list-missing-disk-stores" command to see all disk stores that are being waited on by other members.</br>
</error-text>

<diagnosis> **Diagnosis:**</br>
The above message is typically displayed during start up when a member waits for other members in the cluster to be available, as the table data on disk is not the most current. </br>
The status of the member is displayed as *waiting* in such cases when you [check the status](../howto/check_status_cluster.md) of the cluster using the `snappy-status-all.sh` command.
</diagnosis>

<action> **Solution:** </br>
The status of the waiting members change to online once all the members are online and the status of the waiting members is updated. Users can check whether the status is changed from *waiting* to *online* by using the `snappy-status-all.sh` command or by checking the [SnappyData Monitoring Console](../monitoring/monitoring.md).
</action>

<!-- --------------------------------------------------------------------------- -->

<a id="query-dml-dll"></a>
<error> **Error Message:** </error> 
<error-text>
XCL54.T Query/DML/DDL '{0}' canceled due to low memory on member '{1}'. Try reducing the search space by adding more filter conditions to the where clause. query
</error-text>

<diagnosis> **Diagnosis:**</br>
This error message is reported when a system runs on low available memory. In such cases, the queries may get aborted and an error is reported to prevent the server from crashing due to low available memory.</br>
Once the heap memory usage falls below [critical-heap-percentage](../configuring_cluster/property_description.md#critical-heap-percentage) the queries run successfully.
</diagnosis>

<action> **Solution:** </br>
To avoid such issues, review your memory configuration and make sure that you have allocated enough heap memory. </br>
You can also configure tables for eviction so that table rows are evicted from memory and overflow to disk when the system crosses eviction threshold. For more details refer to best practices for [memory management](../best_practices/memory_management.md)
</action>
<!-- --------------------------------------------------------------------------- -->

<a id="seconds-elapsed"></a>
<error> **Message:** </error>
<error-text>
{0} seconds have elapsed while waiting for reply from {1} on {2} whose current membership list is: [{3}]
</error-text>

<diagnosis> **Diagnosis:**</br>
The above warning message is displayed when a member is awaiting for a response from another member on the system and response has not been received for some time.
</diagnosis>

<action> **Solution:** </br>
This generally means that there is a resource issue in (most likely) the member that is in *waiting* status. Check whether there is a garbage collection activity going on in the member being waited for. 
Due of large GC pauses, the member may not be responding in the stipulated time. In such cases, review your memory configuration and consider whether you can configure to use [off-heap memory](../best_practices/memory_management.md#snappydata-off-heap-memory).
</action>
<!-- --------------------------------------------------------------------------- -->

<a id="persistent-data"></a>
<error> **Error Message:** </error> 
<error-text>
Region {0} bucket {1} has persistent data that is no longer online stored at these locations: {2}
</error-text>

<diagnosis> **Diagnosis:**</br>
In partitioned tables that are persisted to disk, if you have any of the members offline, the partitioned table is still available, but, may have some buckets represented only in offline disk stores. In this case, methods that access the bucket entries report a PartitionOfflineException error.

<action> **Solution:** </br>
If possible, bring the missing member online. This restores the buckets to memory and you can work with them again.
</action>
<!-- --------------------------------------------------------------------------- -->

<a id="no-data-store"></a>
<error> **Error Message:** </error> 
<error-text>
ForcedDisconnectException Error: "No Data Store found in the distributed system for: {0}"
</error-text>

<diagnosis> **Diagnosis:**</br>
A distributed system member’s Cache and DistributedSystem are forcibly closed by the system membership coordinator if it becomes sick or too slow to respond to heartbeat requests. The log file for the member displays a ForcedDisconnectException with the message. </br>
One possible reason for this could be that large GC pauses are causing the member to be unresponsive when the GC is in progress. 
</diagnosis>

<action> **Solution:** </br>
To minimize the chances of this happening, you can increase the DistributedSystem property [member-timeout](../best_practices/important_settings.md#member-timeout). This setting also controls the length of time required to notice a network failure. Also, review your memory configuration and configure to use [off-heap memory](../best_practices/memory_management.md#snappydata-off-heap-memory).
</action>

<!-- --------------------------------------------------------------------------- -->

<a id="queryfailiterate"></a>
<error> **Error Message:** </error> 
<error-text>
Node went down or data no longer available while iterating the results.
</error-text>

<diagnosis> **Diagnosis:**</br>
In cases where a node fails while a JDBC/ODBC client or job is consuming result of a query, then it can result in the query failing with such an exception. 
</diagnosis>

<action> **Solution:** </br>
This is expected behaviour where the product does not retry, since partial results are already consumed by the application. Application must retry the entire query after discarding any changes due to partial results that are consumed.
</action>

<!-- --------------------------------------------------------------------------- -->

<a id="smartconnectorcatalog"></a>
<error> **Message:** </error> 
<error-text>
SmartConnector catalog is not up to date. Please reconstruct the Dataset and retry the operation.
</error-text>

<diagnosis> **Diagnosis:**</br>
In the Smart Connector mode, this error message is seen in the logs if TIBCO ComputeDB catalog is changed due to a DDL operation such as CREATE/DROP/ALTER. 
For performance reasons, TIBCO ComputeDB Smart Connector caches the catalog in the Smart Connector cluster. If there is a catalog change in TIBCO ComputeDB embedded cluster, this error is logged to prevent unexpected errors due to schema changes.
</diagnosis>

<action> **Solution:** </br>
If the user application is performing DataFrame/DataSet operations, you must recreate the DataFrame/DataSet and retry the operation. In such cases, application needs to catch exceptions of type **org.apache.spark.sql.execution.CatalogStaleException** and **java.sql.SQLException** (with SQLState=X0Z39) and retry the operation. Check the following code snippet to get a better understanding of how this scenario should be handled:

```pre
int retryCount = 0;
int maxRetryAttempts = 5;
while (true) {
    try {
        // dataset/dataframe operations goes here (e.g. deleteFrom, putInto)
        return;
    } catch (Exception ex) {
        if (retryCount >= maxRetryAttempts || !isRetriableException(ex)) {
            throw ex;
        } else {
            log.warn("Encountered a retriable exception. Will retry processing batch." +
                " maxRetryAttempts:"+maxRetryAttempts+", retryCount:"+retryCount+"", ex);
            retryCount++;
        }
    }
}

private boolean isRetriableException(Exception ex) {
    Throwable cause = ex;
    do {
        if ((cause instanceof SQLException &&
            ((SQLException)cause).getSQLState().equals("X0Z39"))
            || cause instanceof CatalogStaleException
            || (cause instanceof TaskCompletionListenerException && cause.getMessage()
            .startsWith("java.sql.SQLException: (SQLState=X0Z39"))) {
            return true;
        }
        cause = cause.getCause();
    } while (cause != null);
    return false;
}
```
</action>
<!-- --------------------------------------------------------------------------- -->
<a id="jobsubmitsnap"></a>
<error> **Error Message:** </error> 
<error-text>
Snappy job submission result:
```
{
  "status": "ERROR",
  "result": "Cannot parse config: String: 1: Expecting end of input or a comma, got ':' (if you intended ':' to be part of a key or string value, try enclosing the key or value in double quotes, or you may be able to rename the file .properties rather than .conf)"
}
```
</error-text>

<diagnosis> **Diagnosis:**</br>
This error message is reported when snappy-job submission configuration parameter contains a colon `:`. The typesafe configuration parser (used by job-server) cannot handle `:` as part of key or value unless it is enclosed in double quotes.
Sample job-submit command:

```
./snappy-job.sh submit  --app-name app1 --conf kafka-brokers=localhost:9091 --class Test --app-jar "/path/to/app-jar"
```

!!!Note
	Note that the `kafka-brokers` config value (localhost:9091) is having a `:`
</diagnosis>

<action> **Solution:** </br>
To avoid this issue enclose the value containing colon `:` with double quotes so that the typesafe config parser can parse it. Also since the parameter is passed to the bash script (snappy-job) the quotes needs to be escaped properly otherwise it will get ignored by bash. Check the following example for the correct method of passing this configuration:

```
./snappy-job.sh submit  --app-name app1 --conf kafka-brokers=\"localhost:9091\" --class Test --app-jar "/path/to/app-jar"
```

!!!Note
	Check the value of `kafka-brokers` property enclosed in escaped double quotes: `\"localhost:9091\"`

</action>

<!-- --------------------------------------------------------------------------- -->

<a id="javalangillegal"></a>
<error> **Error Message:** </error> 
<error-text>
java.lang.IllegalStateException: Detected both log4j-over-slf4j.jar AND bound slf4j-log4j12.jar on the class path, preempting StackOverflowError
</error-text>

<diagnosis> **Diagnosis:**</br>
This error message can be seen if application uses SnappyData JDBC driver shadow jar and the application has a dependency on **log4j-over-slf4j** package/jar. This is because, the SnappyData JDBC driver
has a dependency on **slf4j-log4j12** which cannot co-exist with  'log4j-over-slf4j' package.
</diagnosis>

<action> **Solution:** </br>

To avoid getting **log4j** and **slf4j-log4j12** dependencies in the driver, you can link the **non-fat** JDBC client jar (**snappydata-store-client*.jar**) in your application and exclude **log4j** and **slf4j-log4j12** dependencies from it.

Note that the **snappydata-store-client** jar does not contain some of the SnappyData extensions (Scala imiplicits) that are required when SnappyData Spark-JDBC connector is used. That is when accessing SnappyData from another Spark cluster using JDBC dataframes as mentioned [here].(https://snappydatainc.github.io/snappydata/programming_guide/spark_jdbc_connector/#using-sql-dml-to-execute-ad-hoc-sql)). If these SnappyData extensions are to be used, then in addition to above mentioned jar, **snappydata-jdbc*-only.jar** dependency will be required. This is available on maven repo and can be accessed using classifier: 'only' along with snappydata-jdbc cordinates.

Following is an example for adding this dependency using gradle:

build.gradle example that uses **snappydata-store-client jar** and **snappydata-jdbc*only.jar**. The example uses 1.0.2.2 SnappyData version, replace it with the version required by the application.  

**Example**

```

 dependencies {

  compile group: 'io.snappydata', name: 'snappydata-jdbc_2.11', version: '1.0.2.2', classifier: 'only'

  // https://mvnrepository.com/artifact/io.snappydata/snappydata-store-client 
  // SnappyData "no-fat" JDBC jar
  compile group: 'io.snappydata', name: 'snappydata-store-client', version: '1.6.2.1'

  // If users want to add his own 'log4j-over-slf4j' dependency
  compile group: 'org.slf4j', name: 'log4j-over-slf4j', version: '1.7.26'
}

 // exclude the 'log4j' and 'slf4j-log4j12' dependencies
 configurations.all {
    exclude group: 'log4j', module: 'log4j'
    exclude group: 'org.slf4j', module: 'slf4j-log4j12'
 }


```
</action>

<!-- --------------------------------------------------------------------------- --
