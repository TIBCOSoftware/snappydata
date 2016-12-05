##Getting started
---
Before starting with SnappyData getting started guide you can choose between multiple ways to get SnappyData binaries.
Depending on your preference you can work locally on your machine, on AWS or with a docker image.
Preferably you should have 6GB of RAM for this application.

###Getting started with your Spark distribution
---
If you are a Spark developer and is already using Spark 2.0 , fastest way to work with SnappyData is to use "package" option of Spark Shell.
Follow the below instructions step by step.


* From command line go to your Spark installation directory
```scala
$ cd <Spark_Install_dir>
$ ./bin/spark-shell --driver-memory=6g --packages "SnappyDataInc:snappydata:0.6.2-s_2.11"
```
This will open a Spark shell and download the relevant SnappyData files to your local machine. It may take some time to download the files.

<a id="Start_quickStart"></a>

<p>In the following section we will see how to interact with SnappyData.
We will first create a SnappySession, which is the entry point to SnappyData. Then we will see we can create some tables, load data and query the tables from SnappySession.
SnappySession tables have store like behaviour like persistence, redundancy etc. You can check the detailed documentation after trying out this section.
Also, you can choose to work with Scala APIs or SQLs depending on your preference.</p>

#####Getting started using Spark APIs

* Create a SnappySession

 A SnappySession extends SparkSession to work with Row and Column tables.

```scala
scala> val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext, existingSharedState = None)
//Import snappy extensions
scala> import snappy.implicits._
```

* Create a small DataSet using Spark's APIs
```scala
scala> val ds = Seq((1,"a"), (2, "b"), (3, "c")).toDS()
```
* Define a schema for the table.
```scala
scala>  import org.apache.spark.sql.types._
scala>  val tableSchema = StructType(Array(StructField("CustKey", IntegerType, false),
          StructField("CustName", StringType, false)))
```

* Create a column table with a simple schema [String, Int] and default options. For detailed option see [here]
```scala
//Column tables are useful and performs best for analytic class of queries.
scala>  snappy.createTable(tableName = "colTable",
          provider = "column",
          schema = tableSchema,
          options = Map.empty[String, String], // Map for options.
          allowExisting = false)
```

* Insert the created DataSet to the column table "colTable"
```scala
scala>  ds.write.insertInto("colTable")
// Check the total row count.
scala>  snappy.table("colTable").count
```
 Unlike Spark DataFrames SnappyData column tables are mutable. You can insert new rows to a column table.
The following code snippet create a Row object using Spark's API and inserts the Row to the table.

```scala
// Insert a new record
scala>  import org.apache.spark.sql.Row
scala>  snappy.insert("colTable", Row(10, "f"))
// Check the total row count after inserting the row.
scala>  snappy.table("colTable").count
```


* Create a "row" table with a simple schema [String, Int] and default options. For detailed option see [here]

```scala
//Row format tables are used for OLTP class of queries.
scala>  snappy.createTable(tableName = "rowTable",
          provider = "row",
          schema = tableSchema,
          options = Map.empty[String, String],
          allowExisting = false)
```

* Insert the created DataSet to the row table "rowTable"
```scala
scala>  ds.write.insertInto("rowTable")
//Check the row count
scala>  snappy.table("rowTable").count
```
* Insert a new record
```scala
scala>  snappy.insert("rowTable", Row(4, "d"))
//Check the row count now
scala>  snappy.table("rowTable").count
```

* Change some data in a row table.
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

#####Getting started using SQL

We will use SnappySession created above to fire SQL queries

* Create a column table with a simple schema [Int, String] and default options. For detailed option see [here]
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

* Create a row table with primary key

```scala
//Row format tables are used for OLTP class of queries.
scala>  snappy.sql("create table rowTable(CustKey Integer NOT NULL PRIMARY KEY, " +
            "CustName String) using row options()")
```

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

Now that we have seen the basic working of SnappyData tables, let's run [benchmark](#Start Benchmark) code to see the performance of SnappyData.

---

###Getting started by installing SnappyData on-premise
Download the latest version of SnappyData from the
[SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/)
page, which lists the latest and previous releases of SnappyData.

```
$tar -xvf <snappy_binaries>
$cd snappy
$./bin/spark-shell --driver-memory 4g
```
This will open a Spark shell. Then follow the steps mentioned [here](#Start_quickStart)

---

###Getting started on AWS
---
* To be done
###Getting started with Docker image
---
SnappyData comes with a pre-configured container with Docker. The container has binaries for SnappyData. This enables you to try the quickstart program, and more with SnappyData easily.

This section assumes you have already installed Docker and its configured properly.
You can verify it quickly by running.
```scala
$ docker run hello-world

```
Please refer to [Docker documentation](http://docs.docker.com/installation/) for more details.

In addition, make sure that Docker containers have access to at least 4GB of RAM on your machine.

* Type the following command to get the docker image .This will start the container and will take you to the Spark Shell.
```scala
$  docker run -it -p 4040:4040 snappydatainc/snappydata bin/spark-shell --driver-memory 6g
```
It will start downloading the image files to your local machine. It may take some time.
Once your are inside the Spark shell with the "$ scala>" prompt you can follow the steps explained [here](#Start_quickStart)
---

<a id="Start Benchmark"></a>
###SnappyData Query performance
Here we will walk through a simple example to check SnappyData query performance as compared to Spark. We will be creating SnappyData column tables and check query performance
as compared to Spark's DataSet.  Preferably you should have at least 6GB of RAM for the application.

Open your Spark shell. You could be in AWS, docker, on premise installation or using your own Spark's distribution.


* Start a SparkSesion in local mode
```scala
scala>  import org.apache.spark.sql.SparkSession

scala>  val spark = SparkSession.builder.master("local[*]").appName("spark, " +
            "Snappy Quick Start").getOrCreate()
```

* Define a of helper function "benchmark", which will give us an average time of a query after doing initial warmups.
```scala
scala>  def benchmark(name: String, times: Int = 10, warmups: Int = 2)(f: => Unit) {
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
* Create a DataFrame and temp table using Spark's range method. Cache it in Spark to get optimal performance.
  This will create a DataFrame of 100 million records.
```scala
scala>  var df = spark.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
scala>  df.cache
scala>  df.createOrReplaceTempView("sparkCacheTable")
```

* Now run a query and to check the performance. The queries is using average of a field without any where clause.
This will ensure it touches all records while scanning.
```scala
scala>  benchmark("Spark perf") { spark.sql("select avg(k) from sparkCacheTable").collect() }

scala>  spark.sql("select avg(k) from sparkCacheTable").show()

scala>  benchmark("Spark perf") {spark.sql("select avg(k) from sparkCacheTable group by (id%100)").collect}

scala>  spark.sql("select avg(k) from sparkCacheTable group by (id%100)").show
```
* Clean up the JVM. This will ensure all in memory artifacts for Spark is cleaned up.
```scala
scala>  df.unpersist()
scala>  System.gc()
scala>  System.runFinalization()
```

* Create a SnappyContext.
```scala
scala>  val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext, existingSharedState = None)
```
* Create a similar 100 million record DataFrame
```scala
scala>  df = snappy.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
```
* Create a column table in SnappyData. Also insert the created a DataFrame into the table

```scala
scala>  snappy.sql("create table snappyColumnTable (id bigint not null, k bigint not null) using column")
scala>  df.write.insertInto("snappyColumnTable")
```

```scala
//Check the total row count now.
scala>  snappy.sql("select count(*) from snappyColumnTable").show
```



* Now lets run the same benchmark we ran against Spark DataFrame.
```scala
scala>  benchmark("Snappy perf") {snappy.sql("select avg(k) from snappyColumnTable").collect}

scala>  snappy.sql("select avg(k) from snappyColumnTable").show

scala>  benchmark("Snappy perf") {snappy.sql("select avg(k) from snappyColumnTable group by (id%100)").collect}

scala>  snappy.sql("select avg(k) from snappyColumnTable group by (id%100)").show
```

```
scala> :q // Quit the Spark shell
```



