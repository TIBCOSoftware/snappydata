<a id="howto-JSON"></a>
# How to Store and Query JSON Objects

You can insert JSON data in SnappyData tables and execute queries on the tables.

**Code Example: Loads JSON data from a JSON file into a column table and executes query**

The code snippet loads JSON data from a JSON file into a column table and executes the query against it.
The source code for JSON example is located at [WorkingWithJson.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithJson.scala). After creating SnappySession, the JSON file is read using Spark API and loaded into a SnappyData table.

**Get a SnappySession**:

```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("WorkingWithJson")
    .master("local[*]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

**Create a DataFrame from the JSON file**:

```pre
val some_people_path = s"quickstart/src/main/resources/some_people.json"
// Read a JSON file using Spark API
val people = snSession.read.json(some_people_path)
people.printSchema()
```

**Create a SnappyData table and insert the JSON data in it using the DataFrame**:

```pre
//Drop the table if it exists
snSession.dropTable("people", ifExists = true)

//Create a columnar table with the Json DataFrame schema
snSession.createTable(tableName = "people",
  provider = "column",
  schema = people.schema,
  options = Map.empty[String,String],
  allowExisting = false)

// Write the created DataFrame to the columnar table
people.write.insertInto("people")
```

**Append more data from a second JSON file**:

```pre
// Append more people to the column table
val more_people_path = s"quickstart/src/main/resources/more_people.json"

//Explicitly passing schema to handle record level field mismatch
// e.g. some records have "district" field while some do not.
val morePeople = snSession.read.schema(people.schema).json(more_people_path)
morePeople.write.insertInto("people")

//print schema of the table
println("Print Schema of the table\n################")
println(snSession.table("people").schema)
```

**Execute queries and return the results**
```pre
// Query it like any other table
val nameAndAddress = snSession.sql("SELECT " +
    "name, " +
    "address.city, " +
    "address.state, " +
    "address.district, " +
    "address.lane " +
    "FROM people")

nameAndAddress.toJSON.show()
```
