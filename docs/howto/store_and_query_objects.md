<a id="howto-objects"></a>
# How to Store and Query Objects

You can use domain object to load data into SnappyData tables and select the data by executing queries against the table.

**Code Example: Insert Person objects into the column table**

The code snippet below inserts Person objects into a column table. The source code for this example is located at [WorkingWithObjects.scala](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/WorkingWithObjects.scala). After creating SnappySession, the Person objects are inserted using Spark API and loads into a SnappyData table.

**Get a SnappySession**:

```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("CreateReplicatedRowTable")
    .master("local[4]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

**Create DataFrame objects**:
```pre
//Import the implicits for automatic conversion between Objects to DataSets.
import snSession.implicits._

// Create a Dataset using Spark APIs
val people = Seq(Person("Tom", Address("Columbus", "Ohio"), Map("frnd1"-> "8998797979", "frnd2" -> "09878786886"))
  , Person("Ned", Address("San Diego", "California"), Map.empty[String,String])).toDS()
```

**Create a SnappyData table and insert data into it**:

```pre
//Drop the table if it exists.
snSession.dropTable("Persons", ifExists = true)

//Create a columnar table with a Struct to store Address
snSession.sql("CREATE table Persons(name String, address Struct<city: String, state:String>, " +
     "emergencyContacts Map<String,String>) using column options()")

// Write the created DataFrame to the columnar table.
people.write.insertInto("Persons")

//print schema of the table
println("Print Schema of the table\n################")
println(snSession.table("Persons").schema)

// Append more people to the column table
val morePeople = Seq(Person("Jon Snow", Address("Columbus", "Ohio"), Map.empty[String,String]),
  Person("Rob Stark", Address("San Diego", "California"), Map.empty[String,String]),
  Person("Michael", Address("Null", "California"), Map.empty[String,String])).toDS()

morePeople.write.insertInto("Persons")
```

**Execute query on the table and return results**:

```pre
// Query it like any other table
val nameAndAddress = snSession.sql("SELECT name, address, emergencyContacts FROM Persons")

//Reconstruct the objects from obtained Row
val allPersons = nameAndAddress.as[Person]
//allPersons is a Spark Dataset of Person objects. 
// Use of the Dataset APIs to transform, query this data set. 
```
