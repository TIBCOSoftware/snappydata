## Building Snappy applications using Spark API

TIBCO ComputeDB bundles Spark and supports all the Spark APIs. You can create Object based RDDs and run transformations or use the higher level APIs (like Spark ML). All TIBCO ComputeDB managed tables are also accessible as DataFrame and the API extends Spark classes like SQLContext and DataFrames.  So, we recommend getting to know the [concepts in SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html#overview) and the [DataFrame API](http://spark.apache.org/docs/latest/sql-programming-guide.html#dataframes). And, you can store and manage arbitrary RDDs (or even Spark DataSets) through implicit or explicit transformation to a DataFrame. While, the complete SQL support is still evolving, the supported SQL is much richer than SparkSQL. The extension SQL supported by the SnappyStore can be referenced [here](rowAndColumnTables.md).

In Spark SQL, all tables are temporary and cannot be shared across different applications. While you can manage such temporary tables, TIBCO ComputeDB tables are automatically registered to a built-in persistent catalog. This is similar to how Spark SQL uses the Hive catalog to natively work with Hive clusters. Data in tables is primarily managed in-memory with one or more consistent copies across machines or racks, but, can also be reliably managed on disk. 


### SnappyContext
A [SnappyContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext) is the main entry point for TIBCO ComputeDB extensions to Spark. A SnappyContext extends Spark's [SQLContext](http://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.sql.SQLContext) to work with Row and Column tables. Any DataFrame can be managed as TIBCO ComputeDB tables and any table can be accessed as a DataFrame. This is similar to [HiveContext](http://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.sql.hive.HiveContext) - integrates the SQLContext functionality with the Snappy store.


#### Using SnappyContext to create table and query data 
Below are examples to create a SnappyContext from SparkContext.
##### Scala
```pre
  val conf = new org.apache.spark.SparkConf()
               .setAppName("ExampleTest")
               .setMaster("local[*]")

  val sc = new org.apache.spark.SparkContext(conf)
  // get the SnappyContext
  val snc = org.apache.spark.sql.SnappyContext(sc)
```

##### Java
```pre
  SparkConf conf = new org.apache.spark.SparkConf()
               .setAppName("ExampleTest")
               .setMaster("local[*]");

  JavaSparkContext sc = new JavaSparkContext(conf);
  // get the SnappyContext
  SnappyContext snc = SnappyContext.getOrCreate(sc);
```

##### Python
```pre
from pyspark.sql.snappy import SnappyContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("ExampleTest").setMaster("local[*]")
sc = SparkContext(conf=conf)
# get the SnappyContext
snc = SnappyContext(sc)
```
Create columnar tables using API. Other than `create` and `drop` table, rest are all based on the Spark SQL Data Source APIs. 

##### Scala
```pre
  val props1 = Map("BUCKETS" -> "8")  // Number of partitions to use in the SnappyStore
  case class Data(COL1: Int, COL2: Int, COL3: Int)
  val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
  val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

  val dataDF = snc.createDataFrame(rdd)

  // create a column table
  snc.dropTable("COLUMN_TABLE", ifExists = true)

  // "column" is the table format (that is row or column)
  // dataDF.schema provides the schema for table
  snc.createTable("COLUMN_TABLE", "column", dataDF.schema, props1)
  // append dataDF into the table
  dataDF.write.insertInto("COLUMN_TABLE")

  val results1 = snc.sql("SELECT * FROM COLUMN_TABLE")
  println("contents of column table are:")
  results1.foreach(println)
```

##### Java

```pre

    Map<String, String> props1 = new HashMap<>();
    props1.put("buckets", "16");

    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
        RowFactory.create(1,2,3),
        RowFactory.create(7,8,9),
        RowFactory.create(9,2,3),
        RowFactory.create(4,2,3),
        RowFactory.create(5,6,7)
    ));

    StructType schema = new StructType(new StructField[]{
        new StructField("col1", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("col2", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("col3", DataTypes.IntegerType, false, Metadata.empty()),
    });

    DataFrame dataDF = snc.createDataFrame(jrdd, schema);

    // create a column table
    snc.dropTable("COLUMN_TABLE", true);

    // "column" is the table format (that is row or column)
    // dataDF.schema provides the schema for table
    snc.createTable("COLUMN_TABLE", "column", dataDF.schema(), props1, false);
    // append dataDF into the table
    dataDF.write().insertInto("COLUMN_TABLE");

    DataFrame results1 = snc.sql("SELECT * FROM COLUMN_TABLE");
    System.out.println("contents of column table are:");
    for (Row r : results1.select("col1", "col2", "col3"). collect()) {
        System.out.println(r);
    }
```

##### Python

```pre
from pyspark.sql.types import *

data = [(1,2,3),(7,8,9),(9,2,3),(4,2,3),(5,6,7)]
rdd = sc.parallelize(data)
schema=StructType([StructField("col1", IntegerType()), 
                   StructField("col2", IntegerType()), 
                   StructField("col3", IntegerType())])

dataDF = snc.createDataFrame(rdd, schema)

# create a column table
snc.dropTable("COLUMN_TABLE", True)
#"column" is the table format (that is row or column)
#dataDF.schema provides the schema for table
snc.createTable("COLUMN_TABLE", "column", dataDF.schema, True, buckets="16")

#append dataDF into the table
dataDF.write.insertInto("COLUMN_TABLE")
results1 = snc.sql("SELECT * FROM COLUMN_TABLE")

print("contents of column table are:")
results1.select("col1", "col2", "col3"). show()
```


The optional BUCKETS attribute specifies the number of partitions or buckets to use. In SnappyStore, when data migrates between nodes (say if the cluster was expanded) a bucket is the smallest unit that can be moved around. For more details about the properties ('props1' map in above example) and createTable API refer to documentation for [row and column tables](rowAndColumnTables.md)

Create row tables using API, update the contents of row table

```pre
  // create a row format table called ROW_TABLE
  snc.dropTable("ROW_TABLE", ifExists = true)
  // "row" is the table format 
  // dataDF.schema provides the schema for table
  val props2 = Map.empty[String, String]
  snc.createTable("ROW_TABLE", "row", dataDF.schema, props2)

  // append dataDF into the data
  dataDF.write.insertInto("ROW_TABLE")

  val results2 = snc.sql("select * from ROW_TABLE")
  println("contents of row table are:")
  results2.foreach(println)

  // row tables can be mutated
  // for example update "ROW_TABLE" and set col3 to 99 where
  // criteria "col3 = 3" is true using update API
  snc.update("ROW_TABLE", "COL3 = 3", org.apache.spark.sql.Row(99), "COL3" )

  val results3 = snc.sql("SELECT * FROM ROW_TABLE")
  println("contents of row table are after setting col3 = 99 are:")
  results3.foreach(println)

  // update rows using sql update statement
  snc.sql("UPDATE ROW_TABLE SET COL1 = 100 WHERE COL3 = 99")
  val results4 = snc.sql("SELECT * FROM ROW_TABLE")
  println("contents of row table are after setting col1 = 100 are:")
  results4.foreach(println)
```


### SnappyStreamingContext
TIBCO ComputeDB extends Spark streaming so stream definitions can be declaratively written using SQL and these streams can be analyzed using static and dynamic SQL.

Below example shows how to use the SnappyStreamingContext to apply a schema to existing DStream and then query the SchemaDStream with simple SQL. It also shows the SnappyStreamingContext ability to deal with sql queries.

##### Scala
```pre
import org.apache.spark.sql._
import org.apache.spark.streaming._
import scala.collection.mutable
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import scala.collection.immutable.Map

  val snsc = new SnappyStreamingContext(sc, Duration(1))
  val schema = StructType(List(StructField("id", IntegerType) ,StructField("text", StringType)))

  case class ShowCaseSchemaStream (loc:Int, text:String)

  snsc.snappyContext.dropTable("streamingExample", ifExists = true)
  snsc.snappyContext.createTable("streamingExample", "column",  schema, Map.empty[String, String] , false)

  def rddList(start:Int, end:Int) = sc.parallelize(start to end).map(i => ShowCaseSchemaStream( i, s"Text$i"))

  val dstream = snsc.queueStream[ShowCaseSchemaStream](
                mutable.Queue(rddList(1, 10), rddList(10, 20), rddList(20, 30)))

  val schemaDStream = snsc.createSchemaDStream(dstream )

  schemaDStream.foreachDataFrame(df => { 
      df.write.format("column").
      mode(SaveMode.Append).
      options(Map.empty[String, String]).
      saveAsTable("streamingExample")    })
  
  snsc.start()   
  snsc.sql("select count(*) from streamingExample").show
```

##### Python
```pre
from pyspark.streaming.snappy.context import SnappyStreamingContext
from pyspark.sql.types import *

def  rddList(start, end): 
  return sc.parallelize(range(start,  end)).map(lambda i : ( i, "Text" + str(i)))

def saveFunction(df):
   df.write.format("column").mode("append").saveAsTable("streamingExample")
  
schema=StructType([StructField("loc", IntegerType()), 
                   StructField("text", StringType())])

snsc = SnappyStreamingContext(sc, 1)

snsc.queueStream([rddList(1,10) , rddList(10,20), rddList(20,30)])

snsc._snappycontext.dropTable("streamingExample" , True)
snsc._snappycontext.createTable("streamingExample", "column", schema)

dstream = snsc.queueStream(getQueueOfRDDs())
schemadstream = snsc.createSchemaDStream(dstream, schema)
schemadstream.foreachDataFrame(lambda df: saveFunction(df))
snsc.start()

snsc.sql("select count(*) from streamingExample").show()
```
!!! Note:
	Currently Snappy dont have Python API's added for continuous queries and SDE/Sampling.

### Running Spark programs inside the database

<!--
> Note: Above simple example uses local mode (i.e. development mode) to create tables and update data. In the production environment, users will want to deploy the TIBCO ComputeDB system as a unified cluster (default cluster model that consists of servers that embed colocated Spark executors and Snappy stores, locators, and a job server enabled lead node) or as a split cluster (where Spark executors and Snappy stores form independent clusters). Refer to the  [deployment](deployment.md) chapter for all the supported deployment modes and the [configuration](configuration.md) chapter for configuring the cluster. This mode is supported in both Java and Scala. Support for Python is yet not added.-->

To create a job that can be submitted through the job server, the job must implement the _SnappySQLJob or SnappyStreamingJob_ trait. Your job looks like:

##### Scala
```pre
class SnappySampleJob implements SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation
}
```

##### Java
```pre
class SnappySampleJob extends SnappySQLJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  public Object runSnappyJob(SnappyContext snc, Config jobConfig) {//Implementation}

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {//validate}
}

```

##### Scala
```pre
class SnappyStreamingSampleJob implements SnappyStreamingJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  def runSnappyJob(sc: SnappyStreamingContext, jobConfig: Config): Any

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation
}
```

##### Java
```pre
class SnappyStreamingSampleJob extends JavaSnappyStreamingJob {
  /** Snappy uses this as an entry point to execute Snappy jobs. **/
  public Object runSnappyJob(JavaSnappyStreamingContext snsc, Config jobConfig) {//implementation }

  /** TIBCO ComputeDB calls this function to validate the job input and reject invalid job requests **/
  public SnappyJobValidation isValidJob(JavaSnappyStreamingContext snc, Config jobConfig)
  {//validate}
}
```

> The _Job_ traits are simply extensions of the _SparkJob_ implemented by [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver). 

• `runSnappyJob` contains the implementation of the Job.
The [SnappyContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.sql.SnappyContext)/[SnappyStreamingContext](http://snappydatainc.github.io/snappydata/apidocs/#org.apache.spark.streaming.SnappyStreamingContext) is managed by the TIBCO ComputeDB Leader (which runs an instance of Spark JobServer) and will be provided to the job through this method. This relieves the developer from the boiler-plate configuration management that comes with the creation of a Spark job and allows the Job Server to manage and re-use contexts.

• `isValidJob` allows for an initial validation of the context and any provided configuration.
 If the context and configuration are OK to run the job, returning spark.jobserver.SnappyJobValid
  will let the job execute, otherwise returning spark.jobserver.SnappyJobInvalid(reason) prevents
   the job from running and provides means to convey the reason of failure. In this case, the call immediately returns an HTTP/1.1 400 Bad Request status code. validate helps you preventing running jobs that will eventually fail due to missing or wrong configuration and save both time and resources.

See [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) for Spark and spark streaming jobs. 

SnappySQLJob trait extends the SparkJobBase trait. It provides users the singleton SnappyContext object that may be reused across jobs. SnappyContext singleton object creates one SQLContext per incoming SQL connection. Similarly SnappyStreamingJob provides users access to SnappyStreamingContext object that can be reused across jobs



### Submitting Jobs
Following command submits [CreateAndLoadAirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/CreateAndLoadAirlineDataJob.scala) from the [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) directory.   This job creates dataframes from parquet files, loads the data from dataframe into column tables and row tables and creates sample table on column table in its runJob method. The program is compiled into a jar file (quickstart.jar) and submitted to jobs server as shown below.

```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```
The utility snappy-job.sh submits the job and returns a JSON that has a jobId of this job.

- --lead option specifies the host name of the lead node along with the port on which it accepts jobs (8090)
- --app-name option specifies the name given to the submitted app
-  --class specifies the name of the class that contains implementation of the Spark job to be run
-  --app-jar specifies the jar file that packages the code for Spark job

The status returned by the utility is shown below:

```pre
{
  "status": "STARTED",
  "result": {
    "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48",
    "context": "snappyContext1452598154529305363"
  }
}
```
This job ID can be used to query the status of the running job. 
```pre
$ ./bin/snappy-job.sh status  \
    --lead localhost:8090  \
    --job-id 321e5136-4a18-4c4f-b8ab-f3c8f04f0b48

{
  "duration": "17.53 secs",
  "classPath": "io.snappydata.examples.CreateAndLoadAirlineDataJob",
  "startTime": "2016-01-12T16:59:14.746+05:30",
  "context": "snappyContext1452598154529305363",
  "result": "See /home/user1/snappyhome/work/localhost-lead-1/CreateAndLoadAirlineDataJob.out",
  "status": "FINISHED",
  "jobId": "321e5136-4a18-4c4f-b8ab-f3c8f04f0b48"
}
```
Once the tables are created, they can be queried by firing another job. Please refer to [AirlineDataJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/AirlineDataJob.scala) from [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) for the implementation of the job. 
```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.AirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar
```
The status of this job can be queried in the same manner as shown above. The result of the this job will return a file path that has the query results. 

Python users can also submit the python script using spark-submit in split cluster mode. For example below script can be used to read the data loaded by the CreateAndLoadAirlineDataJob. "spark.snappydata.connection" property denotes the locator url of the snappy cluster and it is used to connect to the snappy cluster.
```pre
$ ./bin/spark-submit \
  --master spark://pnq-user02:7077 \
  --conf spark.snappydata.connection=localhost:10334 \
  --conf spark.ui.port=4042  
  python/examples/AirlineDataPythonApp.py
```

### Streaming Jobs

An implementation of SnappyStreamingJob can be submitted to the lead node of TIBCO ComputeDB cluster by specifying `--stream` as an option to the submit command. This option will cause creation of a new SnappyStreamingContext before the job is submitted. Alternatively, user may specify the name of an existing/pre-created streaming context as `--context <context-name>` with the submit command.

For example, [TwitterPopularTagsJob](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/io/snappydata/examples/TwitterPopularTagsJob.scala) from the [examples](https://github.com/SnappyDataInc/snappydata/tree/master/examples/src/main/scala/io/snappydata/examples) directory can be submitted as follows. This job creates stream tables on tweet streams, registers continuous queries and prints results of queries such as top 10 hash tags of last two second, top 10 hash tags until now, top 10 popular tweets.

```pre
$ ./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.TwitterPopularTagsJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar \
    --stream

{
  "status": "STARTED",
  "result": {
    "jobId": "982ac142-3550-41e1-aace-6987cb39fec8",
    "context": "snappyStreamingContext1463987084945028747"
  }
}
```

User needs to stop the currently running streaming job followed by its streaming context if the user intends to submit another streaming job with a new streaming context.

```pre
$ ./bin/snappy-job.sh stop  \
    --lead localhost:8090  \
    --job-id 982ac142-3550-41e1-aace-6987cb39fec8

$ ./bin/snappy-job.sh listcontexts  \
    --lead localhost:8090
["snappyContext1452598154529305363", "snappyStreamingContext1463987084945028747", "snappyStreamingContext"]

$ ./bin/snappy-job.sh stopcontext snappyStreamingContext1463987084945028747  \
    --lead localhost:8090
```
