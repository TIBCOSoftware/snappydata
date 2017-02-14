## Overview of Synopsis Data Engine (SDE)
The SnappyData Synopsis Data Engine (SDE) offers a novel and scalable system to analyze large data sets. SDE uses statistical sampling techniques and probabilistic data structures to answer analytic queries with sub-second latency. There is no need to store or process the entire data set. The approach trades off query accuracy for fast response time. 

For instance, in exploratory analytics, a data analyst might be slicing and dicing large data sets to understand patterns, trends or to introduce new features. Often the results are rendered in a visualization tool through bar charts, map plots and bubble charts. It would increase the productivity of the engineer by providing a near perfect answer that can be rendered in seconds instead of minutes (visually, it is identical to the 100% correct rendering), while the engineer continues to slice and dice the data sets without any interruptions. 

When accessed using a visualization tool (Apache Zeppelin), users immediately get their almost-perfect answer to analytical queries within a couple of seconds, while the full answer can be computed in the background. Depending on the immediate answer, users can choose to cancel the full execution early, if they are either satisfied with the almost-perfect initial answer or if after viewing the initial results they are no longer interested in viewing the final results. This can lead to dramatically higher productivity and significantly less resource consumption in multi-tenant and concurrent workloads on shared clusters.

While in-memory analytics can be fast, it is still expensive and cumbersome to provision large clusters. Instead, SDE allows you to retain data in existing databases and disparate sources, and only caches a fraction of the data using stratified sampling and other techniques. In many cases, data explorers can use their laptops and run high-speed interactive analytics over billions of records. 

Unlike existing optimization techniques based on OLAP cubes or in-memory extracts that can consume a lot of resources and work for a priori known queries, the SnappyData Synopses data structures are designed to work for any ad-hoc query.

### How does it work?
The following diagram provides a simplified view of how the SDE works. The SDE is deeply integrated with the SnappyData store and its general purpose SQL query engine. Incoming rows (could come from static or streaming sources) are continuously sampled into one or more "sample" tables. These samples can be considered much like how a database utilizes indexes - for optimization. There can, however, be one difference, that is, the "exact" table may or may not be managed by SnappyData (for instance, this may be a set of folders in S3 or Hadoop). When queries are executed, the user can optionally specify their tolerance for error through simple SQL extensions. SDE transparently goes through a sample selection process to evaluate if the query can be satisfied within the error constraint. If so, the response is generated directly from the sample. 

![SDE Architecture](Images/sde_architecture.png)

## Key Concepts
SnappyData SDE relies on two methods for approximations - **Stratified Sampling** and **Sketching**. A brief introduction to these concepts is provided below.

###  Stratified Sampling
Sampling is quite intuitive and commonly used by data scientists and explorers. The most common algorithm in use is 'uniform random sampling'. As the term implies, the algorithm is designed to randomly pick a small fraction of the population (the full data set). The algorithm is not biased on any characteristics in the data set. It is totally random and the probability of any element being selected in the sample is the same (or uniform). But, uniform random sampling does not work well for general purpose querying.

Take this simple example table that manages AdImpressions. If we create a random sample that is a third of the original size we pick two records in random. 
This is depicted in the following figure:

![Uniform Random Sampling](Images/aqp_stratifiedsampling1.png)

If we run a query like 'SELECT avg(bid) FROM AdImpresssions where geo = 'VT'', the answer is a 100% wrong. The common solution to this problem could be to increase the size of the sample. 

![Uniform Random Sampling](Images/aqp_stratifiedsampling2.png)

But, if the data distribution along this 'GEO' dimension is skewed, you could still keep picking any records or have too few records to produce a good answer to queries. 

Stratified sampling, on the other hand, allows the user to specify the common dimensions used for querying and ensures that each dimension or strata have enough representation in the sampled data set. For instance, as shown in the following figure, a sample stratified on 'Geo' would provide a much better answer. 

![Stratified Sampling](Images/aqp_stratifiedsampling3.png)

To understand these concepts in further detail, refer to the [handbook](https://web.eecs.umich.edu/~mozafari/php/data/uploads/approx_chapter.pdf). It explains different sampling strategies, error estimation mechanisms, and various types of data synopses.

### Online Sampling
SDE also supports continuous sampling over streaming data and not just static data sets. For instance, you can use the Spark DataFrame APIs to create a uniform random sample over static RDDs. For online sampling, SDE first does [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) for each startum in a write-optimized store before flushing it into a read-optimized store for stratified samples. 
There is also explicit support for time series. For instance, if AdImpressions are continuously streaming in, we can ensure that we have enough samples over each 5-minute time window, while still ensuring that all GEOs have good representation in the sample. 

### Sketching
While stratified sampling ensures that data dimensions with low representation are captured, it still does not work well when you want to capture outliers. For instance, queries like 'Find the top-10 users with the most re-tweets in the last 5 minutes may not result in good answers. Instead, we use rely on other data structures like a Count-min-sketch to capture data frequencies in a stream. This is a data structure that requires that it captures how often we see an element in a stream for the top-N such elements. 
While a [Count-min-sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) is well described, SDE extends this with support for providing top-K estimates over time series data. 

## Working with Stratified Samples

###Create Sample Tables###

You can create sample tables on datasets that can be sourced from any source supported in Spark/SnappyData. For instance, these can be SnappyData in-memory tables, Spark DataFrames, or sourced from an external data source such as S3 or HDFS. 

Here is an SQL based example to create a sample on tables locally available in the SnappyData cluster. 

```
CREATE SAMPLE TABLE NYCTAXI_PICKUP_SAMPLE ON NYCTAXI 
  OPTIONS (qcs 'hour(pickup_datetime)', fraction '0.01') 
  AS (SELECT * FROM NYCTAXI);

CREATE SAMPLE TABLE TAXIFARE_HACK_LICENSE_SAMPLE on TAXIFARE 
  OPTIONS (qcs 'hack_license', fraction '0.01') 
  AS (SELECT * FROM TAXIFARE);
```
Often your data set is too large to also fit in available cluster memory. If so, you can create an external table pointing to the source. 
In this example below, a sample table is created for an S3 (external) dataset:

```
CREATE EXTERNAL TABLE TAXIFARE USING parquet 
  OPTIONS(path 's3a://<AWS_SECRET_ACCESS_KEY>:<AWS_ACCESS_KEY_ID>@zeppelindemo/nyctaxifaredata_cleaned');
//Next, create the sample sourced from this table ..
CREATE SAMPLE TABLE TAXIFARE_HACK_LICENSE_SAMPLE on TAXIFARE 
  options  (qcs 'hack_license', fraction '0.01') AS (SELECT * FROM TAXIFARE);

```
When creating a base table, if you have applied the **partition by** clause, the clause is also applied to the sample table. The sample table also inherits the **number of buckets**, **redundancy** and **persistence** properties from the base table.

For sample tables, the **overflow** property is set to **False** by default. (For column tables the default value is  **True**). 

For example:

```
CREATE TABLE BASETABLENAME <column details> 
USING COLUMN OPTIONS (partition_by '<column_name_a>', Buckets '7', Redundancy '1')

CREATE TABLE SAMPLETABLENAME <column details> 
USING COLUMN_SAMPLE OPTIONS (qcs '<column_name_b>',fraction '0.05', 
strataReservoirSize '50', baseTable 'baseTableName')
// In this case, sample table 'sampleTableName' is partitioned by column 'column_name_a', has 7 buckets and 1 redundancy.

```


###QCS (Query Column Set) and Sample Selection###
For stratified samples, you are required to specify the columns used for stratification(QCS) and how big the sample needs to be (fraction). 

QCS, which stands for Query Column Set is typically the most commonly used dimensions in your query GroupBy/Where and Having clauses. A QCS can also be constructed using SQL expressions - for instance, using a function like `hour (pickup_datetime)`.

The parameter *fraction* represents the fraction of the full population that is managed in the sample. Intuition tells us that higher the fraction, more accurate the answers. But, interestingly, with large data volumes, you can get pretty accurate answers with a very small fraction. With most data sets that follow a normal distribution, the error rate for aggregations exponentially drops with the fraction. So, at some point, doubling the fraction does not drop the error rate. SDE always attempts to adjust its sampling rate for each stratum so that there is enough representation for all sub-groups. 
For instance, in the above example, taxi drivers that have very few records may actually be sampled at a rate much higher than 1% while very active drivers (a lot of records) is automatically sampled at a lower rate. The algorithm always attempts to maintain the overall 1% fraction specified in the 'create sample' statement. 

One can create multiple sample tables using different sample QCS and sample fraction for a given base table. 

Here are some general guidelines to use when creating samples:

* Note that samples are only applicable when running aggregation queries. For point lookups or selective queries, the engine automatically rejects all samples and runs the query on the base table. These queries typically would execute optimally anyway on the underlying data store.

* Start by identifying the most common columns used in GroupBy/Where and Having clauses. 

* Then, identify a subset of these columns where the cardinality is not too large. For instance, in the example above we picked 'hack_license' (one license per driver) as the strata and we sample 1% of the records associated with each driver. 

* Avoid using unique columns or timestamps for your QCS. For instance, in the example above, 'pickup_datetime' is a time stamp and is not a good candidate given its likely hood of high cardinality. That is, there is a possibility that each record in the Dataset has a different timesstamp. Instead, when dealing with time series we use the 'hour' function to capture data for each hour. 

* When the accuracy of queries is not acceptable, add more samples using the common columns used in GroupBy/Where clauses as mentioned above. The system automatically picks the appropriate sample. 

<Note> Note: The value of the QCS column should not be empty or set to null for stratified sampling, or an error may be reported when the query is executed.</Note>


## Running Queries

Queries can be executed directly on sample tables or on the base table. Any query executed on the sample directly will always result in an approximate answer. When queries are executed on the base table users can specify their error tolerance and additional behavior to permit approximate answers. The Engine will automatically figure out if the query can be executed by any of the available samples. If not, the query can be executed on the base table based on the behavior clause. 

Here is the syntax:

> > SELECT ... FROM .. WHERE .. GROUP BY ...<br>
> > WITH ERROR `<fraction> `[CONFIDENCE` <fraction>`] [BEHAVIOR `<string>]`
    
* **WITH ERROR** - this is a mandatory clause. The values are  0 < value(double) < 1 . 
* **CONFIDENCE** - this is optional clause. The values are confidence 0 < value(double) < 1 . The default value is 0.95
* **BEHAVIOR** - this is an optional clause. The values are `do_nothing`, `local_omit`, `strict`,  `run_on_full_table`, `partial_run_on_base_table`. The default value is `run_on_full_table`	

These 'behavior' options are fully described in the section below. 

Here are some examples:

```
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc 
  with error 0.10 
// tolerate a maximum error of 10% in each row in the answer with a default confidence level of 0.95.

SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc 
  with error 
// tolerate any error in the answer. Just give me a quick response.

SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc with error 0.10 confidence 0.95 behavior ‘local_omit’
// tolerate a maximum error of 10% in each row in the answer with a confidence interval of 0.95.
// If the error for any row is greater than 10% omit the answer. i.e. the row is omitted. 
```
#### Using the Spark DataFrame API

We extend the Spark DataFrame API with support for approximate queries. Here is 'withError' API on DataFrames.
```
def withError(error: Double,
confidence: Double = Constant.DEFAULT_CONFIDENCE,
behavior: String = "DO_NOTHING"): DataFrame
```

Query examples using the DataFrame API
``` 
snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).orderBy( desc("Month_")).withError(0.10) 
snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).orderBy( desc("Month_")).withError(0.10, 0.95, 'local_omit’) 
```

### Supporting BI tools or existing Apps
To allow BI tools and existing Apps that say might be generating SQL, SDE also supports specifying these options through your SQL connection or using the Snappy SQLContext. 

```
snContext.sql(s"spark.sql.aqp.error=$error")
snContext.sql(s"spark.sql.aqp.confidence=$confidence")
snContext.sql(s"set spark.sql.aqp.behavior=$behavior")
```
These settings will apply to all queries executed via this SQLContext. Application can override this by also using the SQL extensions specified above.

Applications or tools using JDBC/ODBC can set the following properties. 
For example, when using Apache Zeppelin JDBC interpreter or the snappy-shell you can set the values as below:

```
set spark.sql.aqp.error=$error;
set spark.sql.aqp.confidence=$confidence;
set spark.sql.aqp.behavior=$behavior;
```

## More Examples
###Example 1
Create a sample table with qcs 'medallion' 
```
CREATE SAMPLE TABLE NYCTAXI_SAMPLEMEDALLION ON NYCTAXI 
  OPTIONS (buckets '7', qcs 'medallion', fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM NYCTAXI);
```

**SQL Query:**
```
select medallion,avg(trip_distance) as avgTripDist,
  absolute_error(avgTripDist),relative_error(avgTripDist),
  lower_bound(avgTripDist),upper_bound(avgTripDist) 
  from nyctaxi group by medallion order by medallion desc limit 100
  with error;
  // We explain these built-in error functions in a section below.
```

**DataFrame API Query:**
```
snc.table(basetable).groupBy("medallion").agg( avg("trip_distance").alias("avgTripDist"),
  absolute_error("avgTripDist"),  relative_error("avgTripDist"), lower_bound("avgTripDist"),
  upper_bound("avgTripDist")).withError(.6, .90, "do_nothing").sort(col("medallion").desc).limit(100)
```

###Example 2
Create additional sample table with qcs 'hack_license' 

```
CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE ON NYCTAXI OPTIONS
(buckets '7', qcs 'hack_license', fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM NYCTAXI);
```

**SQL Query:**
```
select  hack_license, count(*) count from NYCTAXI group by hack_license order by count desc limit 10 with error
// the engine will automitically use the HackLicense sample for a more accurate answer to this query.
```

**DataFrame API Query:**
```
snc.table(basetable).groupBy("hack_license").count().withError(.6,.90,"do_nothing").sort(col("count").desc).limit(10)
```

###Example 3
Create a sample table using function "hour(pickup_datetime) as QCS

```
Sample Tablecreate sample table nyctaxi_hourly_sample on nyctaxi options (buckets '7', qcs 'hourOfDay', fraction '0.01', strataReservoirSize '50') AS (select *, hour(pickupdatetime) as hourOfDay from nyctaxi);
```

**SQL Query:**
```
select sum(trip_time_in_secs)/60 totalTimeDrivingInHour, hour(pickup_datetime) from nyctaxi group by hour(pickup_datetime)
```

**DataFrame API Query:**
```
snc.table(basetable).groupBy(hour(col("pickup_datetime"))).agg(Map("trip_time_in_secs" -> "sum")).withError(0.6,0.90,"do_nothing").limit(10)
```

###Example 4
If you want a higher assurance of accurate answers for your query, match the QCS to "group by columns" followed by any filter condition columns. Here is a sample using multiple columns.

```
Sample Tablecreate sample table nyctaxi_hourly_sample on nyctaxi options (buckets '7', qcs 'hack_license, year(pickup_datetime), month(pickup_datetime)', fraction '0.01', strataReservoirSize '50') AS (select *, hour(pickupdatetime) as hourOfDay from nyctaxi);
```

**SQL Query:**
```
Select hack_license, sum(trip_distance) as daily_trips from nyctaxi  where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 group by hack_license  order by daily_trips desc
```

**DataFrame API Query:**
```
snc.table(basetable).groupBy("hack_license","pickup_datetime").agg(Map("trip_distance" -> "sum")).alias("daily_trips").       filter(year(col("pickup_datetime")).equalTo(2013) and month(col("pickup_datetime")).equalTo(9)).withError(0.6,0.90,"do_nothing").sort(col("sum(trip_distance)").desc).limit(10)
```

##Sample Selection

Sample selection logic selects most appropriate sample, based on this relatively simple logic in the current version:

* If the query is not an aggregation query (based on COUNT, AVG, SUM) then reject the use of any samples. The query is executed on the base table. Else,

* If query QCS (columns involved in Where/GroupBy/Having matched the sample QCS, then, select that sample

* If exact match is not available, then, if the sample QCS is a superset of query QCS, that sample is used

* If superset of sample QCS is not available, a sample where the sample QCS is a subset of query QCS is used

* When multiple stratified samples with a subset of QCSs match, a sample with most matching columns is used. The largest size of the sample gets selected if multiple such samples are available. 


##High-level Accuracy Contracts (HAC)###
SnappyData combines state-of-the-art approximate query processing techniques and a variety of data synopses to ensure interactive analytics over both, streaming and stored data. Using high-level accuracy contracts (HAC), SnappyData offers end users intuitive means for expressing their accuracy requirements, without overwhelming them with statistical concepts.

When an error constraint is not met, the action to be taken is defined in the behavior clause. 

###Behavior Clause
Synopsis Data Engine has HAC support using the following behavior clause. 

#### `<do_nothing>`
The SDE engine returns the estimate as is. 

![DO NOTHING](Images/aqp_donothing.png)

#### `<local_omit>`
For aggregates that do not satisfy the error criteria, the value is replaced by a special value like "null". 
![LOCAL OMIT](Images/aqp_localomit.png)


#### `<strict>`#####
If any of the aggregate column in any of the rows do not meet the HAC requirement, the system throws an exception. 
![Strict](Images/aqp_strict.png)

#### `<run_on_full_table>`
If any of the single output row exceeds the specified error, then the full query is re-executed on the base table.

![RUN OF FULL TABLE](Images/aqp_runonfulltable.png)

#### `<partial_run_on_base_table>`
If the error is more than what is specified in the query, for any of the output rows (that is sub-groups for a group by query), the query is re-executed on the base table for those sub-groups.  This result is then merged (without any duplicates) with the result derived from the sample table. 
![PARTIAL RUN ON BASE TABLE](Images/aqp_partialrunonbasetable.png)

In the following example, any one of the above behavior clause can be applied. 

```
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_  with error <fraction> [CONFIDENCE <fraction>] [BEHAVIOR <behavior>]
```

###Error Functions###
In addition to this, SnappyData supports error functions that can be specified in the query projection. These error functions are supported for the SUM, AVG and COUNT aggregates in the projection. 

The following four methods are available to be used in query projection when running approximate queries:

* **absolute_error(column alias**) : Indicates absolute error present in the estimate (approx answer) calculated using error estimation method (ClosedForm or Bootstrap) 

* **relative_error(column alias)** : Indicates ratio of absolute error to estimate.

* **lower_bound(column alias)** : Lower value of an estimate interval for a given confidence.

* **upper_bound(column alias)**: Upper value of an estimate interval for a given confidence.

Confidence is the probability that the value of a parameter falls within a specified range of values.

For example:

```
SELECT avg(ArrDelay) as AvgArr ,absolute_error(AvgArr),relative_error(AvgArr),lower_bound(AvgArr), upper_bound(AvgArr),
UniqueCarrier FROM airline GROUP BY UniqueCarrier order by UniqueCarrier WITH ERROR 0.12 confidence 0.9
```
* The `absolute_error` and `relative_error` function values returns 0 if query is executed on the base table. 
* `lower_bound` and `upper_bound` values returns null if query is executed on the base table. 
* The values are seen in case behavior is set to `<run_on_full_table>` or`<partial_run_on_base_table>`

In addition to using SQL syntax in the queries, you can use data frame API as well. 
For example, if you have a data frame for the airline table, then the below query can equivalently also be written as :

```
select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay), absolute_error(arrivalDelay), Year_ from airline group by Year_ order by Year_ with error 0.10 confidence 0.95
```

```
snc.table(basetable).groupBy("Year_").agg( avg("ArrDelay").alias("arrivalDelay), relative_error("arrivalDelay"), absolute_error("arrivalDelay"), col("Year_")).withError(0.10, .95).sort(col("Year_").asc) 
```

###Reserved Keywords ###
Keywords are predefined reserved words that have special meanings and cannot be used in a paragraph. Keyword `sample_` is reserved for SnappyData.

If the aggregate function is aliased in the query as `sample_<any string>`, then what you get is true answers on the sample table, and not the estimates of the base table.

`select count() rowCount, count() as sample_count from airline with error 0.1`

rowCount returns estimate of a number of rows in airline table.
sample_count returns a number of rows (true answer) in sample table of airline table.


## Sketching 
Synopses data structures are typically much smaller than the base data sets that they represent. They use very little space and provide fast, approximate answers to queries. A [BloomFilter](https://en.wikipedia.org/wiki/Bloom_filter) is a commonly used example of a synopsis data structure. Another example of a synopsis structure is a [Count-Min-Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) which serves as a frequency table of events in a stream of data. The ability to use Time as a dimension for querying makes synopses structures much more useful. As streams are ingested, all relevant synopses are updated incrementally and can be queried using SQL or the Scala API.

### Creating TopK tables###
TopK queries are used to rank attributes to answer "best, most interesting, most important" class of questions. TopK structures store elements ranking them based on their relevance to the query. [TopK](http://stevehanov.ca/blog/index.php?id=122) queries aim to retrieve, from a potentially very large resultset, only the *k (k >= 1)* best answers.
 
####SQL API for creating a TopK table in SnappyData
 
``` 
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets')")
``` 
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables.

####Scala API for creating a TopK table
   
	
	val topKOptionMap = Map(
        "epoch" -> System.currentTimeMillis().toString,
        "timeInterval" -> "1000ms",
        "size" -> "40",
        "frequencyCol" -> "retweets"
      )
      val schema = StructType(List(StructField("HashTag", StringType)))
      snc.createApproxTSTopK("MostPopularTweets", Some("tweetStreamTable"),
        "HashTag", schema, topKOptionMap)
	  
The code above shows how to do the same thing using the SnappyData Scala API.
  
####Querying the TopK table
	
	
	select * from topkTweets order by EstimatedValue desc 
	
The example above queries the TopK table which returns the top 40 (the depth of the TopK table was set to 40) hashtags with the most re-tweets.

### Approximate TopK analytics for time series data###
Time is used as an attribute in creating the TopK structures. Time can be an attribute of the incoming data set (which is frequently the case with streaming data sets) and in the absence of that, the system uses arrival time of the batch as the time stamp for that incoming batch. The TopK structure is populated along the dimension of time. As an example, the most re-tweeted hashtags in each window are stored in the data structure. This allows us to issue queries like, "what are the most popular hashtags in a given time interval?" Queries of this nature are typically difficult to execute and not easy to optimize (due to space considerations) in a traditional system.

Here is an example of a time-based query on the TopK structure which returns the most popular hashtags in the time interval queried. The SnappyData SDE module provides two attributes startTime and endTime which can be used to run queries on arbitrary time intervals.
	
	
	select hashtag, EstimatedValue, ErrorBoundsInfo from MostPopularTweets where 
        startTime='2016-01-26 10:07:26.121' and endTime='2016-01-26 11:14:06.121' 
        order by EstimatedValue desc
	  
	
If time is an attribute in the incoming data set, it can be used instead of the system generated time. In order to do this, the TopK table creation is provided the name of the column containing the timestamp.

####SQL API for creating a TopK table in SnappyData specifying timestampColumn

In the example below tweetTime is a field in the incoming dataset which carries the timestamp of the tweet.
 
```scala
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets', timeSeriesColumn 'tweetTime' )")
``` 
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest re-tweets value in the base table. This works for both static tables and streaming tables

####Scala API for creating a TopK table 

```scala
    val topKOptionMap = Map(
        "epoch" -> System.currentTimeMillis().toString,
        "timeInterval" -> "1000ms",
        "size" -> "40",
        "frequencyCol" -> "retweets",
        "timeSeriesColumn" -> "tweetTime"
      )
      val schema = StructType(List(StructField("HashTag", StringType)))
      snc.createApproxTSTopK("MostPopularTweets", Some("tweetStreamTable"),
        "HashTag", schema, topKOptionMap)
```

The code above shows how to do the same thing using the SnappyData Scala API.

It is worth noting that the user has the ability to disable time as a dimension if desired. This is done by not providing the *timeInterval* attribute when creating the TopK table.

##Using SDE##
In the current release SDE queries only work for SUM, AVG and COUNT aggregations. Joins are only supported to non-samples in this release. The SnappyData SDE module will gradually expand the scope of queries that can be serviced through it. But the overarching goal here is to dramatically cut down on the load on current systems by diverting at least some queries to the sampling subsystem and increasing productivity through fast response times. 
