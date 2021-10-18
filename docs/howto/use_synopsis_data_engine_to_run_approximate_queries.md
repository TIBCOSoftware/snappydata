<a id="howto-sde"></a>
# How to use Approximate Query Processing (AQP) to Run Approximate Queries

Approximate Query Processing (AQP) uses statistical sampling techniques and probabilistic data structures to answer analytic queries with sub-second latency. There is no need to store or process the entire dataset. The approach trades off query accuracy for fast response time.
For more information on  AQP, refer to [AQP documentation](../sde/index.md).

**Code Example**:
The complete code example for AQP is in [SynopsisDataExample.scala](https://github.com/TIBCOSoftware/snappydata/blob/master/examples/src/main/scala/org/apache/spark/examples/snappydata/SynopsisDataExample.scala). The code below creates a sample table and executes queries that run on the sample table.

**Get a SnappySession**:
```pre
val spark: SparkSession = SparkSession
    .builder
    .appName("SynopsisDataExample")
    .master("local[*]")
    .getOrCreate

val snSession = new SnappySession(spark.sparkContext)
```

**The base column table(AIRLINE) is created from temporary parquet table as follows**:

```pre
// Create temporary staging table to load parquet data
snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINE " +
    "USING parquet OPTIONS(path " + s"'${dataFolder}/airlineParquetData')")

// Create a column table AIRLINE
snSession.sql("CREATE TABLE AIRLINE USING column AS (SELECT Year AS Year_, " +
    "Month AS Month_ , DayOfMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, " +
    "CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, " +
    "CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, " +
    "TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, " +
    "WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay, " +
    "ArrDelaySlot FROM STAGING_AIRLINE)")
```

**Create a sample table for the above base table**:
Attribute 'qcs' in the statement below specifies the columns used for stratification and attribute 'fraction' specifies how big the sample needs to be (3% of the base table AIRLINE in this case). For more information on Approximate Query Processing, refer to the [AQP documentation](../sde/index.md#working-with-stratified-samples).


```pre
snSession.sql("CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE OPTIONS" +
    "(qcs 'UniqueCarrier, Year_, Month_', fraction '0.03')  " +
    "AS (SELECT Year_, Month_ , DayOfMonth, " +
    "DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, " +
    "FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, " +
    "ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, " +
    "Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, " +
    "NASDelay, SecurityDelay, LateAircraftDelay, ArrDelaySlot FROM AIRLINE)")
```

**Execute queries that return approximate results using sample tables**:
The query below returns airlines by number of flights in descending order. The 'with error 0.20' clause in the query below signals query engine to execute the query on the sample table instead of the base table and maximum 20% error is allowed.

```pre
var result = snSession.sql("select  count(*) flightRecCount, description AirlineName, " +
    "UniqueCarrier carrierCode ,Year_ from airline , airlineref where " +
    "airline.UniqueCarrier = airlineref.code group by " +
    "UniqueCarrier,description, Year_ order by flightRecCount desc limit " +
    "10 with error 0.20").collect()
result.foreach(r => println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))
```

**Join the sample table with a reference table**:
You can join the sample table with a reference table to execute queries. The example below illustrates how a reference table (AIRLINEREF) is created as from a parquet data file.
```pre
// create temporary staging table to load parquet data
snSession.sql("CREATE EXTERNAL TABLE STAGING_AIRLINEREF USING " +
    "parquet OPTIONS(path " + s"'${dataFolder}/airportcodeParquetData')")
snSession.sql("CREATE TABLE AIRLINEREF USING row AS (SELECT CODE, " +
    "DESCRIPTION FROM STAGING_AIRLINEREF)")
```
**Join the sample table and reference table to find out which airlines arrive on schedule**:

```pre
result = snSession.sql("select AVG(ArrDelay) arrivalDelay, " +
    "relative_error(arrivalDelay) rel_err, description AirlineName, " +
    "UniqueCarrier carrier from airline, airlineref " +
    "where airline.UniqueCarrier = airlineref.Code " +
    "group by UniqueCarrier, description order by arrivalDelay " +
    "with error").collect()
   result.foreach(r => println(r(0) + ", " + r(1) + ", " + r(2) + ", " + r(3)))
```
