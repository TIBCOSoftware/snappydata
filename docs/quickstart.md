#Getting Started in 5 Minutes or Less

You have multiple choices for getting started with SnappyData. Depending on your preference you can work locally on your local machine, on premise network, on AWS or with a docker image. We will adding support for Azure in the near future. 
If you decide to get going on your local machine you should have 6GB of RAM for SnappyData.

##Getting Started with your Spark Distribution

If you are a Spark developer and is already using Spark 2.0 , fastest way to work with SnappyData is to add SnappyData as a dependency. For instance, using "package" option of Spark Shell.

Follow the below instructions to try out SnappyData in 5 minutes or less. We encourage you to also try out the quick performance benchmark to see the 10X advantage over Spark's native caching performance. 


**Open a command terminal. and go to the location of the Spark installation directory:**
```scala
$ cd <Spark_Install_dir>
$ ./bin/spark-shell --packages "SnappyDataInc:snappydata:0.7-s_2.11"
```

This opens a Spark shell and downloads the relevant SnappyData files to your local machine. It may take some time to download the files. 

<a id="Start_quickStart"></a>
The following section provides details of how to interact with SnappyData:

Your application code is almost all Spark. You bootstrap SnappyData using a SnappySession (derived from SparkSession). In this simple test, we create some tables, load data and query the tables using Spark SQL.
Tables in SnappyData exhibit many operational capabilities like disk persistence, redundancy for HA, eviction, etc. You can check the [detailed documentation](programming_guide.md#ddl) after trying out this section. 

While SnappyData supports Scala/Java/Python/SQL APIs for this quickstart you can choose to work with Scala APIs or SQL depending on your preference.

##Getting Started using Spark Scala APIs

**Create a SnappySession** :A SnappySession extends SparkSession so you can mutate data, get much higher performance, etc.

```scala
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
//Import snappy extensions
scala> import snappy.implicits._
```

**Create a small dataSet using Spark's APIs**
```scala
scala> val ds = Seq((1,"a"), (2, "b"), (3, "c")).toDS()
```
**Define a schema for the table**
```scala
scala>  import org.apache.spark.sql.types._
scala>  val tableSchema = StructType(Array(StructField("CustKey", IntegerType, false),
          StructField("CustName", StringType, false)))
```

**Create a column table with a simple schema [String, Int] and default options**. For detailed option refer to the 
```scala
//Column tables manage data is columnar form and offer superier performance for analytic class queries.
scala>  snappy.createTable(tableName = "colTable",
          provider = "column", // Create a SnappyData Column table
          schema = tableSchema,
          options = Map.empty[String, String], // Map for options.
          allowExisting = false)
```
SnappyData (SnappySession) extends SparkSession so you can simply use all the Spark APIs

**Insert the created DataSet to the column table "colTable"**
```scala
scala>  ds.write.insertInto("colTable")
// Check the total row count.
scala>  snappy.table("colTable").count
```
**Create a row object using Spark's API and inserts the Row to the table**
Unlike Spark DataFrames SnappyData column tables are mutable. You can insert new rows to a column table.

```scala
// Insert a new record
scala>  import org.apache.spark.sql.Row
scala>  snappy.insert("colTable", Row(10, "f"))
// Check the total row count after inserting the row.
scala>  snappy.table("colTable").count
```

**Create a "row" table with a simple schema [String, Int] and default options. <br>**For detailed option refer to the [Row and Column Tables](programming_guide.md#tables-in-snappydata) section.

```scala
//Row formatted tables are better when data sets constantly change or access is selective (like based on a key).
scala>  snappy.createTable(tableName = "rowTable",
          provider = "row",
          schema = tableSchema,
          options = Map.empty[String, String],
          allowExisting = false)
```

**Insert the created DataSet to the row table "rowTable"**
```scala
scala>  ds.write.insertInto("rowTable")
//Check the row count
scala>  snappy.table("rowTable").count
```
**Insert a new record**
```scala
scala>  snappy.insert("rowTable", Row(4, "d"))
//Check the row count now
scala>  snappy.table("rowTable").count
```

**Change some data in a row table**
```scala
//Updating a row for customer with custKey = 1
scala>  snappy.update(tableName = "rowTable", filterExpr = "CUSTKEY=1",
                newColumnValues = Row("d"), updateColumns = "CUSTNAME")

scala>  snappy.table("rowTable").orderBy("CUSTKEY").show

//Delete the row for customer with custKey = 1
scala>  snappy.delete(tableName = "rowTable", filterExpr = "CUSTKEY=1")

```

```scala
//Drop the existing tables
scala>  snappy.dropTable("rowTable", ifExists = true)
scala>  snappy.dropTable("colTable", ifExists = true)
```

##Getting Started using SQL

We illustrate SQL using Spark SQL invoked using the Session API. You can also use any SQL client tool (e.g. Snappy Shell; example in the How-to section).

Create a column table with a simple schema [Int, String] and default options. For details on the options refer to the [Row and Column Tables](programming_guide.md#tables-in-snappydata) section.
```scala
scala>  snappy.sql("create table colTable(CustKey Integer, CustName String) using column options()")
```

```
//Insert couple of records to the column table
scala>  snappy.sql("insert into colTable values(1, 'a')")
scala>  snappy.sql("insert into colTable values(2, 'b')")
scala>  snappy.sql("insert into colTable values(3, '3')")
```

```scala
// Check the total row count now.
scala>  snappy.sql("select count(*) from colTable").show
```

**Create a row table with primary key**

```scala
//Row formatted tables are better when data sets constantly change or access is selective (like based on a key).
scala>  snappy.sql("create table rowTable(CustKey Integer NOT NULL PRIMARY KEY, " +
            "CustName String) using row options()")
```
If you create a table using standard SQL (i.e. no 'row options' clause) it will create a replicated Row table.
 
```scala
//Insert couple of records to the row table
scala>  snappy.sql("insert into rowTable values(1, 'a')")
scala>  snappy.sql("insert into rowTable values(2, 'b')")
scala>  snappy.sql("insert into rowTable values(3, '3')")
```

```scala
//Update some rows
scala>  snappy.sql("update rowTable set CustName='d' where custkey = 1")
scala>  snappy.sql("select * from rowTable order by custkey").show
```


```scala
//Drop the existing tables
scala>  snappy.sql("drop table if exists rowTable ")
scala>  snappy.sql("drop table if exists colTable ")
```

```
scala> :q //Quit the Spark shell
```

Now that we have seen the basic working of SnappyData tables, let's run [benchmark](#Start Benchmark) code to see the performance of SnappyData and compare it to Spark's native cache performance.

##Getting Started by Installing SnappyData On-Premise
Download the latest version of SnappyData from the [SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/) page, which lists the latest and previous releases of SnappyData.

```
$tar -xvf <snappy_binaries>
$cd snappy
$./bin/spark-shell --driver-memory 4g
```
It opens a Spark shell. Follow the steps mentioned [here](#Start_quickStart)


##Getting Started on AWS

You can quickly create a single host SnappyData cluster (i.e. one lead node , one data node and a locator in a single machine) through the AWS CloudFormation.


###Prerequisites###
Before you begin,:

* Ensure that you have an existing AWS account with required permissions to launch EC2 resources
* Sign in to the AWS console using your AWS account-specific URL. This ensures that the account-specific URL is stored as a cookie in the browser, which then redirects you to the appropriate AWS URL for subsequent logins.
*  Create an EC2 key pair in the region where you want to launch the SnappyData Cloud cluster


Launch the instance from [here](https://console.aws.amazon.com/cloudformation/home#/stacks/new?bucket=snappydata-cloudbuilder&templateURL=https://zeppelindemo.s3.amazonaws.com/quickstart/snappydata-quickstart.json)

1. The Login Screen is displayed. Enter your AWS login credentials. <br> Your default region is displayed automatically. You can change the region in the console.
 
2. The **Select Template page** is displayed. The URL for the template (JSON format) is pre-populated. Click **Next** to continue.   
<p style="text-align: center;"><img alt="STEP" src="/Images/cluster_selecttemplate.png"></p>
<br>

3. On the **Specify Details** page, you can:<br>
    * Change the stack name: Enter a name for the stack
    * Select Instance Type: By default the c4.2xlarge instance (with 8 CPU core and 15 GB RAM) is selected. This is the recommended instance size for running this quickstart.
    * Select KeyPairName: Select a keypair from the list of keypairs available to you. This is a mandatory field.
    * Search VPCID: Select the VPC ID from the dropdown list. Your instance(s) is launched within this VPC. This is a mandatory field.<br> 
<Note> NOTE: The stack name must contain only letters, numbers, dashes and should start with an alpha character.</Note>
<p style="text-align: center;"><img alt="Refresh" src="/Images/cluster_specifydetails.png"></p>

4. Click **Next**. <br>

5. On the **Options** page, click **Next** to continue using the provided default value.<br>

6. On the **Review** page, verify the details and click **Create** to create a stack. <br>
<Note> NOTE: This operation may take a few minutes to complete.  </Note>
<p style="text-align: center;"><img alt="Create" src="/Images/cluster_createstack.png"></p>
<a id="Stack"></a>

7. The next page lists the existing stacks. Click **Refresh** to view the updated list and the status of the stack creation. 
When the cluster has started, the status of the stack changes to **CREATE_COMPLETE**. <br>
<p style="text-align: center;"><img alt="Refresh" src="/Images/cluster_refresh.png"></p>
<a id="Stack"></a>
8. Click on the **Outputs** tab, to view the links (URL) required for launching Apache Zeppelin, which provides web-based notebooks for data exploration. <br>
	<p style="text-align: center;"><img alt="Public IP" src="/Images/cluster_links.png"></p>
<Note>Note: If the status of the stack displays **ROLLBACK_IN_PROGRESS** or **DELETE_COMPLETE**, the stack creation may have failed. Some common problems that might have caused the failure are:

	> * **Insufficient Permissions**: Verify that you have the required permissions for creating a stack (and other AWS resources) on AWS.
	> * **Invalid Keypair**: Verify that the EC2 keypair exists in the region you selected in the iSight CloudBuilder creation steps.
	> * **Limit Exceeded**: Verify that you have not exceeded your resource limit. For example, if you exceed the allocated limit of Amazon EC2 instances, the resource creation fails and an error is reported.
</Note>
For more information, refer to the [Apache Zeppelin](#LoggingZeppelin) section or refer to the [Apache Zeppelin documentation](http://zeppelin.apache.org/).


<Note>Note: </Note>

* <Note> Multi-node cluster set up on AWS via CloudFormation will be supported in future releases. However, users can set up a multi-node cluster using the EC2 scripts.</Note>
* <Note>To stop incurring charges for the instance, you can either terminate the instance or delete the stack. You cannot connect to or restart an instance after you have terminated it.</Note>

##Getting Started with Docker Image

SnappyData comes with a pre-configured container with Docker. The container has binaries for SnappyData. This enables you to try the quickstart program and more with SnappyData easily.

This section assumes you have already installed Docker and its configured properly.
You can verify it quickly by running.
```scala
$ docker run hello-world

```
Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

In addition, make sure that Docker containers have access to at least 4GB of RAM on your machine.

* Type the following command to get the docker image .This will start the container and will take you to the Spark Shell.
```scala
$  docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory 6g
```
<mark>Jags: This Docker image name is NOT CORRECT. It should have the release version number associated with it. Dhaval needs to fix. </mark>

It will start downloading the image files to your local machine. It may take some time.
Once your are inside the Spark shell with the "$ scala>" prompt you can follow the steps explained [here](#Start_quickStart)


<a id="Start Benchmark"></a>
##20X Faster than Spark 2.0 Caching
Here we will walk through a simple benchmark to compare SnappyData to Spark 2.0 performance. We load millions of rows into a cached Spark DataFrame, run some analytic queries measuring its performance and then do the same using SnappyData's column table. 
 Preferably you should have at least 8GB of RAM reserved for this test.

Start the Spark shell using any of the options mentioned below.

*  If your are using your own Spark 2.0 installation 

    ```scala
    $ ./bin/spark-shell --driver-memory=8g --packages "SnappyDataInc:snappydata:0.7.0-s_2.11" --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=2g"
    ```

*  If you have downloaded Snappydata 

    ```scala
    $ ./bin/spark-shell --driver-memory=8g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=2g"
    ```
* If your are using docker

    ```scala
    $ docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory=8g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=2g"
    ```

You should be in a Spark shell. Then follow the instruction below to get the performance numbers.

* Define a function "benchmark", which tells us the average time to run queries after doing initial warmups.
```scala
scala>  def benchmark(name: String, times: Int = 10, warmups: Int = 6)(f: => Unit) {
          for (i <- 1 to warmups) {
            f
          }
          val startTime = System.nanoTime
          for (i <- 1 to times) {
            f
          }
          val endTime = System.nanoTime
          println(s"Average time taken in $name for $times runs: " +
            (endTime - startTime).toDouble / (times * 1000000.0) + " millis")
        }
```
<mark>Jags: 50 million is not sufficient to showcase the performance difference? Rishi, can u check? 8gb can hold much more.</mark>

* Create a DataFrame and temp table using Spark's range method. Cache it in Spark to get optimal performance. This creates a DataFrame of 50 million records.
```scala
scala>  var testDF = spark.range(50000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
scala>  testDF.cache
scala>  testDF.createOrReplaceTempView("sparkCacheTable")
```

* Now run a query and to check the performance. The queries is using average of a field without any where clause.
This will ensure it touches all records while scanning.
```scala
scala>  benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}
```
* Clean up the JVM. This will ensure all in memory artifacts for Spark is cleaned up.
```scala
scala>  testDF.unpersist()
scala>  System.gc()
scala>  System.runFinalization()
```

* Create a SnappyContext
```scala
scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
```
* Create a similar 50 million record DataFrame
```scala
scala>  testDF = snappy.range(50000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")
```
* Create the table
```scala
scala>  snappy.sql("drop table if exists snappyTable")
scala>  snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")
```
* Insert the created a DataFrame into the table. Measure its performance
```scala
scala>  benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }
```

* Now lets run the same benchmark we ran against Spark DataFrame.
```scala
scala>  benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}
```

```
scala> :q // Quit the Spark shell
```
For more examples of the common operations you can refer to the [How tos](howto.md) section. 

If you have questions or queries you can contact us through our [community channels](techsupport.md#community).
