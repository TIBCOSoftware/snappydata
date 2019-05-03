# Building TIBCO ComputeDB Applications using Spark API

## Snappy Session Usage
### Create Columnar Tables using API 
Other than `create` and `drop` table, rest are all based on the Spark SQL Data Source APIs.

### Scala
```pre
 val props = Map("BUCKETS" -> "8")// Number of partitions to use in the Snappy Store

 case class Data(COL1: Int, COL2: Int, COL3: Int)

 val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
 val rdd = spark.sparkContext.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

 val df = snappy.createDataFrame(rdd)

 // create a column table
 snappy.dropTable("COLUMN_TABLE", ifExists = true)

 // "column" is the table format (that is row or column)
 // dataDF.schema provides the schema for table
 snappy.createTable("COLUMN_TABLE", "column", df.schema, props)
 // append dataDF into the table
 df.write.insertInto("COLUMN_TABLE")

 val results = snappy.sql("SELECT * FROM COLUMN_TABLE")
 println("contents of column table are:")
 results.foreach(r => println(r))
```

### Java
```pre
 Map<String, String> props1 = new HashMap<>();
 props1.put("buckets", "16");

 JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
  RowFactory.create(1, 2, 3),
  RowFactory.create(7, 8, 9),
  RowFactory.create(9, 2, 3),
  RowFactory.create(4, 2, 3),
  RowFactory.create(5, 6, 7)
 ));

 StructType schema = new StructType(new StructField[]{
  new StructField("col1", DataTypes.IntegerType, false, Metadata.empty()),
  new StructField("col2", DataTypes.IntegerType, false, Metadata.empty()),
  new StructField("col3", DataTypes.IntegerType, false, Metadata.empty()),
 });

 Dataset<Row> df = snappy.createDataFrame(jrdd, schema);

// create a column table
 snappy.dropTable("COLUMN_TABLE", true);

// "column" is the table format (that is row or column)
// dataDF.schema provides the schema for table
 snappy.createTable("COLUMN_TABLE", "column", df.schema(), props1, false);
// append dataDF into the table
 df.write().insertInto("COLUMN_TABLE");

 Dataset<Row>  results = snappy.sql("SELECT * FROM COLUMN_TABLE");
 System.out.println("contents of column table are:");
 for (Row r : results.select("col1", "col2", "col3"). collectAsList()) {
   System.out.println(r);
 }
```


### Python

```pre
from pyspark.sql.types import *

data = [(1,2,3),(7,8,9),(9,2,3),(4,2,3),(5,6,7)]
rdd = sc.parallelize(data)
schema=StructType([StructField("col1", IntegerType()),
                   StructField("col2", IntegerType()),
                   StructField("col3", IntegerType())])

dataDF = snappy.createDataFrame(rdd, schema)

# create a column table
snappy.dropTable("COLUMN_TABLE", True)
#"column" is the table format (that is row or column)
#dataDF.schema provides the schema for table
snappy.createTable("COLUMN_TABLE", "column", dataDF.schema, True, buckets="16")

#append dataDF into the table
dataDF.write.insertInto("COLUMN_TABLE")
results1 = snappy.sql("SELECT * FROM COLUMN_TABLE")

print("contents of column table are:")
results1.select("col1", "col2", "col3"). show()
```

The optional BUCKETS attribute specifies the number of partitions or buckets to use. In Snappy Store, when data migrates between nodes (say if the cluster is expanded) a bucket is the smallest unit that can be moved around. 
For more details about the properties ('props1' map in above example) and `createTable` API refer to the documentation for [row and column tables](#tables-in-snappydata).

## Create Row Tables using API, Update the Contents of Row Table

```pre
// create a row format table called ROW_TABLE
snappy.dropTable("ROW_TABLE", ifExists = true)
// "row" is the table format
// dataDF.schema provides the schema for table
val props2 = Map.empty[String, String]
snappy.createTable("ROW_TABLE", "row", dataDF.schema, props2)

// append dataDF into the data
dataDF.write.insertInto("ROW_TABLE")

val results2 = snappy.sql("select * from ROW_TABLE")
println("contents of row table are:")
results2.foreach(println)

// row tables can be mutated
// for example update "ROW_TABLE" and set col3 to 99 where
// criteria "col3 = 3" is true using update API
snappy.update("ROW_TABLE", "COL3 = 3", org.apache.spark.sql.Row(99), "COL3" )

val results3 = snappy.sql("SELECT * FROM ROW_TABLE")
println("contents of row table are after setting col3 = 99 are:")
results3.foreach(println)

// update rows using sql update statement
snappy.sql("UPDATE ROW_TABLE SET COL1 = 100 WHERE COL3 = 99")
val results4 = snappy.sql("SELECT * FROM ROW_TABLE")
println("contents of row table are after setting col1 = 100 are:")
results4.foreach(println)
```

## Using SnappyStreamingContext 

TIBCO ComputeDB extends Spark streaming so stream definitions can be declaratively written using SQL and these streams can be analyzed using static and dynamic SQL.


### Scala
```pre
 import org.apache.spark.sql._
 import org.apache.spark.streaming._
 import scala.collection.mutable
 import org.apache.spark.rdd._
 import org.apache.spark.sql.types._
 import scala.collection.immutable.Map

 val snsc = new SnappyStreamingContext(spark.sparkContext, Duration(1))
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

### Java
```pre
 StructType schema = new StructType(new StructField[]{
     new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
     new StructField("text", DataTypes.StringType, false, Metadata.empty())
 });

 Map<String, String> props = Collections.emptyMap();
 jsnsc.snappySession().dropTable("streamingExample", true);
 jsnsc.snappySession().createTable("streamingExample", "column", schema, props, false);

 Queue<JavaRDD<ShowCaseSchemaStream>> rddQueue = new LinkedList<>();// Define a JavaBean named ShowCaseSchemaStream
 rddQueue.add(rddList(jsc, 1, 10));
 rddQueue.add(rddList(jsc, 10, 20));
 rddQueue.add(rddList(jsc, 20, 30));
 
 //rddList methods is defined as
/* private static JavaRDD<ShowCaseSchemaStream> rddList(JavaSparkContext jsc, int start, int end){
    List<ShowCaseSchemaStream> objs = new ArrayList<>();
      for(int i= start; i<=end; i++){
        objs.add(new ShowCaseSchemaStream(i, String.format("Text %d",i)));
      }
    return jsc.parallelize(objs);
 }*/

 JavaDStream<ShowCaseSchemaStream> dStream = jsnsc.queueStream(rddQueue);
 SchemaDStream schemaDStream = jsnsc.createSchemaDStream(dStream, ShowCaseSchemaStream.class);

 schemaDStream.foreachDataFrame(new VoidFunction<Dataset<Row>>() {
   @Override
   public void call(Dataset<Row> df) {
     df.write().insertInto("streamingExample");
   }
 });

 jsnsc.start();

 jsnsc.sql("select count(*) from streamingExample").show();
```

### Python
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

dstream = snsc.queueStream([rddList(1,10) , rddList(10,20), rddList(20,30)])

snsc._snappycontext.dropTable("streamingExample" , True)
snsc._snappycontext.createTable("streamingExample", "column", schema)

schemadstream = snsc.createSchemaDStream(dstream, schema)
schemadstream.foreachDataFrame(lambda df: saveFunction(df))
snsc.start()
time.sleep(1)
snsc.sql("select count(*) from streamingExample").show()
```

<!--
> Note: Above simple example uses local mode (i.e. development mode) to create tables and update data. In the production environment, users will want to deploy the TIBCO ComputeDB system as a unified cluster (default cluster model that consists of servers that embed colocated Spark executors and Snappy Stores, locators, and a job server enabled lead node) or as a split cluster (where Spark executors and TIBCO ComputeDB stores form independent clusters). Refer to the  [deployment](../deployment.md) chapter for all the supported deployment modes and the [configuration](../configuring_cluster/configuring_cluster.md) chapter for configuring the cluster. This mode is supported in both Java and Scala. Support for Python is yet not added.-->

