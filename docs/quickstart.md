#Getting Started in 5 Minutes or Less

Welcome to the Getting Started section! <br>
We provide you multiple choices for getting started with SnappyData. 
Depending on your preference you can try any of the following options:

* [Getting Started with your Spark Distribution](#getting-started-with-your-spark-distribution)
* [Getting Started Using Spark Scala APIs](#getting-started-using-spark-scala-apis)
* [20X Faster than Spark 2.0 Caching](#20x-faster-than-spark-20-caching)
* [Getting Started using SQL](#getting-started-using-sql)
* [Getting Started by Installing SnappyData On-Premise](#getting-started-by-installing-snappydata-on-premise)
* [Getting Started on AWS](#getting-started-on-aws)
* [Getting Started with Docker Image](#getting-started-with-docker-image)

<Note>Note: Support for Microsoft Azure will be provided in future releases</Note>

##Getting Started with your Spark Distribution

If you are a Spark developer and already using Spark 2.0, the fastest way to work with SnappyData is to add SnappyData as a dependency. For instance, using "package" option of Spark Shell.

This section contains instructions and examples using which, you can try out SnappyData in 5 minutes or less. We encourage you to also try out the quick performance benchmark to see the 10X advantage over Spark's native caching performance. 


**Open a command terminal and go to the location of the Spark installation directory:**
```scala
$ cd <Spark_Install_dir>
$ ./bin/spark-shell --packages "SnappyDataInc:snappydata:0.7-s_2.11"
```

This opens a Spark Shell and downloads the relevant SnappyData files to your local machine. Depending on your network connection, it may take some time to download the files. 

###Interacting with SnappyData
<a id="Start_quickStart"></a>
The following section provides details of how to interact with SnappyData. 
Most of your application code is in Spark. You bootstrap SnappyData using a SnappySession (derived from SparkSession). In this simple test, we create some tables, load data and query the tables using Spark SQL.

Tables in SnappyData exhibit many operational capabilities like disk persistence, redundancy for HA, eviction, etc. For more information, you can refer to the [detailed documentation](programming_guide.md#ddl). 

While SnappyData supports Scala, Java, Python, SQL APIs for this quick start you can choose to work with Scala APIs or SQL depending on your preference.

##Getting Started Using Spark Scala APIs

**Create a SnappySession**: A SnappySession extends SparkSession so you can mutate data, get much higher performance, etc.

```scala
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
//Import snappy extensions
scala> import snappy.implicits._
```

**Create a small dataset using Spark's APIs**
```scala
scala> val ds = Seq((1,"a"), (2, "b"), (3, "c")).toDS()
```
**Define a schema for the table**
```scala
scala>  import org.apache.spark.sql.types._
scala>  val tableSchema = StructType(Array(StructField("CustKey", IntegerType, false),
          StructField("CustName", StringType, false)))
```

**Create a column table with a simple schema [String, Int] and default options**. 
For detailed option refer to the [Row and Column Tables](programming_guide.md#tables-in-snappydata) section.
```scala
//Column tables manage data is columnar form and offer superier performance for analytic class queries.
scala>  snappy.createTable(tableName = "colTable",
          provider = "column", // Create a SnappyData Column table
          schema = tableSchema,
          options = Map.empty[String, String], // Map for options.
          allowExisting = false)
```
SnappyData (SnappySession) extends SparkSession, so you can simply use all the Spark's APIs.

**Insert the created DataSet to the column table "colTable""**
```scala
scala>  ds.write.insertInto("colTable")
// Check the total row count.
scala>  snappy.table("colTable").count
```
**Create a row object using Spark's API and insert the Row to the table**
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
//Row formatted tables are better when datasets constantly change or access is selective (like based on a key).
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

<a id="Start Benchmark"></a>
##20X Faster than Spark 2.0 Caching
Here we walk you through a simple benchmark to compare SnappyData to Spark 2.0 performance. 
We load millions of rows into a cached Spark DataFrame, run some analytic queries measuring its performance and then, repeat the same using SnappyData's column table. 

 <Note> Note: Preferably you should have at least 4GB of RAM reserved for this test.</Note>

**Start the Spark Shell using any of the options mentioned below:**

**If you are using your own Spark 2.0 installation:**

```scala 
$ ./bin/spark-shell --driver-memory=4g --packages "SnappyDataInc:snappydata:0.7.0-s_2.11" --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

**If you have downloaded SnappyData **

```scala
$ ./bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```

** If you are using Docker**
```scala
$ docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g"
```
### To get the Performance Numbers
Ensure that you are in a Spark Shell, and then follow the instruction below to get the performance numbers.

**Define a function "benchmark"**, which tells us the average time to run queries after doing initial warm ups.
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

**Create a DataFrame and temp table using Spark's range method** <br>Cache it in Spark to get optimal performance. This creates a DataFrame of 100 million records.You can change number of rows based on  your memory availability.
```scala
scala>  var testDF = spark.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
scala>  testDF.cache
scala>  testDF.createOrReplaceTempView("sparkCacheTable")
```

**Run a query and to check the performance** <br>
The queries is using average of a field, without any where clause. This ensures that it touches all records while scanning.
```scala
scala>  benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}
```

**Clean up the JVM**. This ensures that all in memory artifacts for Spark is cleaned up.
```scala
scala>  testDF.unpersist()
scala>  System.gc()
scala>  System.runFinalization()
```

**Create a SnappyContex**t
```scala
scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
```

** Create similar 50 million record DataFrame**
```scala
scala>  testDF = snappy.range(50000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")
```

**Create the table**
```scala
scala>  snappy.sql("drop table if exists snappyTable")
scala>  snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")
```

**Insert the created DataFrame into the table and measure its performance**
```scala
scala>  benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }
```

**Now let us run the same benchmark against Spark DataFrame**
```scala
scala>  benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}
```

```
scala> :q // Quit the Spark Shell
```

##Getting Started using SQL

We illustrate SQL using Spark SQL invoked using the Session API. You can also use any SQL client tool (e.g. Snappy Shell; example in the [How-to](howto/#howto-snappyShell) section).

**Create a column table with a simple schema [Int, String] and default options. **For details on the options refer to the [Row and Column Tables](programming_guide.md#tables-in-snappydata) section.
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
// Check the total row count now
scala>  snappy.sql("select count(*) from colTable").show
```

**Create a row table with primary key**

```scala
//Row formatted tables are better when datasets constantly change or access is selective (like based on a key).
scala>  snappy.sql("create table rowTable(CustKey Integer NOT NULL PRIMARY KEY, " +
            "CustName String) using row options()")
```
If you create a table using standard SQL (i.e. no 'row options' clause) it creates a replicated Row table.
 
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
scala> :q //Quit the Spark Shell
```

Now that we have seen the basic working of SnappyData tables, let's run the [benchmark](#Start Benchmark) code to see the performance of SnappyData and compare it to Spark's native cache performance.

##Getting Started by Installing SnappyData On-Premise
Download the latest version of SnappyData from the [SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/) page, which lists the latest and previous releases of SnappyData.

```
$tar -xvf <snappy_binaries>
$cd snappy
$./bin/spark-shell
```
It opens a Spark Shell. Follow the steps mentioned [here](#Start_quickStart)


##Getting Started on AWS

You can quickly create a single host SnappyData cluster (i.e. one lead node, one data node and a locator in a single EC2 instance) through the AWS CloudFormation.


###Prerequisites###

Before you begin:

* Ensure that you have an existing AWS account with required permissions to launch EC2 resources from CloudFormation

* Sign in to the AWS console using your AWS account-specific URL. This ensures that the account-specific URL is stored as a cookie in the browser, which then redirects you to the appropriate AWS URL for subsequent logins.

*  Create an EC2 Key Pair in the region where you want to launch the SnappyData Cloud cluster

To launch the cluster from EC2 click [here](https://console.aws.amazon.com/cloudformation/home#/stacks/new?templateURL=https://zeppelindemo.s3.amazonaws.com/quickstart/snappydata-quickstart.json) and follow the instructions below.

1. The AWS Login Screen is displayed. Enter your AWS login credentials. 
 
2. The **Select Template page** is displayed. The URL for the template (JSON format) is pre-populated. Click **Next** to continue.
<note> Note: You are placed in your default region. You can either continue in the selected region or change it in the console. </Note>
<p style="text-align: center;"><img alt="STEP" src="/Images/cluster_selecttemplate.png"></p>
<br>


3. On the **Specify Details** page, you can:<br>
    * Provide the stack name: Enter a name for the stack. The stack name must contain only letters, numbers, dashes and should start with an alpha character. This is a mandatory field.
	* Select Instance Type: By default the c4.2xlarge instance (with 8 CPU core and 15 GB RAM) is selected. This is the recommended instance size for running this quickstart.
    * Select KeyPairName: Select a keypair from the list of keypairs available to you. This is a mandatory field.
    * Search VPCID: Select the VPC ID from the dropdown list. Your instance(s) is launched within this VPC. This is a mandatory field.<br> 
<p style="text-align: center;"><img alt="Refresh" src="/Images/cluster_specifydetails.png"></p>

4. Click **Next**. <br>

5. On the **Options** page, click **Next** to continue using the provided default values.<br>

6. On the **Review** page, verify the details and click **Create** to create a stack. <br>
<p style="text-align: center;"><img alt="Create" src="/Images/cluster_createstack.png"></p>
<a id="Stack"></a>


7. The next page lists the existing stacks. Click **Refresh** to view the updated list. Select the stack to view its status. 
When the cluster has started, the status of the stack changes to **CREATE_COMPLETE**. This process may take 4-5 minutes to complete.<br>
<p style="text-align: center;"><img alt="Refresh" src="/Images/cluster_refresh.png"></p>
<a id="Stack"></a>
<Note>Note: If the status of the stack displays as **ROLLBACK_IN_PROGRESS** or **DELETE_COMPLETE**, the stack creation may have failed. Some common causes behind the failure are:

	> * **Insufficient Permissions**: Verify that you have the required permissions for creating a stack (and other AWS resources) on AWS.
	> * **Invalid Keypair**: Verify that the EC2 keypair exists in the region you selected in the iSight CloudBuilder creation steps.
	> * **Limit Exceeded**: Verify that you have not exceeded your resource limit. For example, if you exceed the allocated limit of Amazon EC2 instances, the resource creation fails and an error is reported.
</Note>

9. Your cluster is now running. You can explore it using Apache Zeppelin, which provides web-based notebooks for data exploration. The Apache Zeppelin server has already been started on the instance for you. Simply follow its link (URL) from the **Outputs** tab.<br>
	<p style="text-align: center;"><img alt="Public IP" src="/Images/cluster_links.png"></p>

For more information, refer to the [Apache Zeppelin](#LoggingZeppelin) section or refer to the [Apache Zeppelin documentation](http://zeppelin.apache.org/).


<Note>Note: </Note>

* <Note> Multi-node cluster set up on AWS via CloudFormation will be supported in future releases. However, users can set it up using the [EC2 scripts](install.md#setting-up-cluster-on-aws).</Note>
* <Note>To stop incurring charges for the instance, you can either terminate the instance or delete the stack after you are done playing with the cluster. However, you cannot connect to or restart an instance after you have terminated it.</Note>

##Getting Started with Docker Image

SnappyData comes with a pre-configured container with Docker. The container has binaries for SnappyData. This enables you to easily try the quick start program and more, with SnappyData.

This section assumes you have already installed Docker and its configured properly. Refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

**Verify that Docker is installed**: In the command prompt run the command:
```scala
$ docker run hello-world

```

<Note>Note: Ensure that the Docker containers have access to at least 4GB of RAM on your machine</Note>

**Get the Docker Image: ** In the command prompt, type the following command to get the docker image. This starts the container and takes you to the Spark Shell.
```scala
$  docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory 6g
```
It starts downloading the image files to your local machine. Depending on your network connection, it may take some time.
Once your are inside the Spark Shell with the "$ scala>" prompt, you can follow the steps explained [here](#Start_quickStart)


####More Information

For more examples of the common operations you can refer to the [How tos](howto.md) section. 

If you have questions or queries you can contact us through our [community channels](techsupport.md#community).
