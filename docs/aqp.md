# Overview of Synopsis Data Engine#
Data volumes from transactional and non-transactional sources have grown exponentially, and as a result, conventional data processing and data visualization technologies have struggled to provide real time analysis on these ever increasing data sets. 

Conventional wisdom has relied on iterating over the entire data set to produce complete and accurate results, while experiencing longer wait times to provide those insights. Data scientists and data engineers agree that when it comes to exploratory analytics, the ability to quickly get a directionally correct answer is more important than waiting for long periods of time to get a complete accurate answer. This is partly because in most cases, the decision to pursue a certain hypothesis does not change a whole lot whether you get back a 99% accurate answer or a 100% accurate answer.

The SnappyData Synopsis Data Engine (SDE) offers a novel and scalable solution to the data volume problem. SDE uses statistical sampling techniques and probabilistic data structures to answer aggregate class queries, without needing to store or operate over the entire data set. The approach trades off query accuracy for quicker response times, allowing for queries to be run on large data sets with meaningful and accurate error information. 

Using SDE, practitioners can populate the engine with synopses of large data sets, and run SQL queries to get answers orders of magnitude faster than querying the complete data set. This approach significantly reduces memory requirements, cuts down I/O by an order of magnitude, requires less CPU overall (especially when compared to dealing with large data sets) and reduces query latencies by an order of magnitude. With these improvements, users can now perform analytics faster without being encumbered by complex infrastructure, massive data load times and long query response times.


For example, for research-based companies (like Gallup), for political polls results, a small sample is used to estimate support for a candidate within a small margin of error

The following diagram provides a general framework of the Synopsis Data Engine:

A small random sample of the rows of the original database table is prepared. Queries are directed against this small sample table, and then approximate results are generated based on the query and error estimation.
![Example](./Images/sde_architecture.png)

Thus, there are two components in the architecture, a component for building the synopses from database relations, and a component that rewrites an incoming query to use the synopses to answer the query approximately and to report with an estimate of the error in the answer. 

##Synopsis Data Engine Technique 1: Sampling##
###What is Sampling ?###
One commonly-used technique for approximate results is sampling. For many aggregation queries, non-uniform (approximate) samples can provide more accurate approximations than a uniform sample. 

For example, if you want to find the top selling products in a sales database or the fraction of people who study in a specific area, evaluating the entire product or population would be impractical due to factors like restrictions on time, cost etc.
Sampling provides a solution, where a small sample of data, which represents the entire data is randomly selected. In this case, a query is answered based on the pre-sampled small amount of data, and then scaled up based on the sample rate.

Sampling-based systems have the advantage that they can be implemented as a thin layer of middleware which re-writes queries to run against sample tables stored as ordinary relations in a standard, off-the-shelf database server. 

The two techniques that the SnappyData AQP module uses to accomplish this are, **reservoir sampling** as applied to **stratified sampling**. 

####Reservoir Sampling ####
Reservoir Sampling is an algorithm for sampling elements from a stream of data, where a random sample of elements are returned, which are evenly distributed from the original stream.

####Stratified Sampling ####
This refers to a type of sampling method where the data is divided into separate groups, called strata. A simple random sample is then drawn from each group. 

###Key Concepts###
**Data Synopses**: During the pre-processing phase, data synopses (or data structures) are built over the database. These data base synopses are used when queries are issued to the system, and approximate results are returned.

**Strata**:  A specific procedure for biased sampling, where the database is partitioned into different strata, and each stratum is uniformly sampled at different sampling rates.


###QCS (Query Column Set) and Sample selection###
We term the columns used for grouping and filtering in a query (used in GROUP BY/WHERE/HAVING clauses) as the query column set or query QCS. Columns used for stratification during the sampling are termed as sample QCS. One can also use functions as sample QCS column. For example, hour (pickup_datetime)

General guideline to select sample QCS is to look for columns in a table, which are generally used in grouping or filtering of queries. This results in good representation of data in sample for each sub-group of data in query and approximate results are closer to the actual results. Generally, columns which have low cardinality should be provided as QCS columns for good representation data in sample tables.

For example, for month of the year (only 12 unique values) or unique-carriers of airlines (limited in number) .

> ###Note: The value of the QCS column should not empty or set to null for stratified sampling, or an error may be reported when the query is executed.

Let us take a look at this example:
```
CREATE SAMPLE TABLE NYCTAXI_pickup_sample ON NYCTAXI  OPTIONS ( qcs 'hour(pickup_datetime)', fraction '0.01') AS (SELECT * FROM NYCTAXI);
```

Here* fraction* represents how much fraction of a base table data goes into the sample table. Higher the fraction, more the memory requirement and larger the query execution time. In this case, the results increase as fraction increases, so it is a trade-off the user has to think about, while selecting a fraction based on the applications requirements.

One can create multiple sample tables using different sample QCS and sample fraction for a given base table. Sample selection logic selects most appropriate table based on the following logic:


###Sample Selection:###
* If query QCS is exactly the same as a sample of the given QCS, then, that sample gets selected.
* If exact match is not available, then, if the QCS of the sample is a superset of query QCS, that sample is used.
* If superset of sample QCS is not available, a sample where the sample QCS is subset of query, QCS is used

When multiple stratified samples with subset of QCSs match, sample where most number of columns match with query QCS is used. Largest size of sample gets selected if multiple such samples are available. 

For example: If query QCS are A, B and C. If samples with QCS  A&B and B&C are available, then choose a sample with large sample size.


###Using Error Functions and Confidence Interval in Queries###
Acceptable error fraction and expected confidence interval can be specified in the query projection. 
A query can end with the clauses **WITH ERROR** and **WITH ERROR `<fraction>`[CONFIDENCE `<fraction>`] [BEHAVIOR` <string>]`**
####Using “WITH ERROR “Clause####
In this clause, context level setting can be overridden by query level settings. When this clause is specified, the query is run with the following values:

```
	ERROR 0.2 
	CONFIDENCE 0.95 
	BEHAVIOR 'do_nothing'
```	

For example: 

```
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order 
by Month_ with error 
```
These values can be overridden by setting in the  SnappyData  context below.	

```
snContext.sql(s"spark.sql.aqp.error=$error")
snContext.sql(s"spark.sql.aqp.confidence=$confidence")
snContext.sql(s"set spark.sql.aqp.behavior=$behavior")
```

Apache Zeppelin or snappy-shell can use the set context level values as below:

```
set spark.sql.aqp.error=$error;
set spark.sql.aqp.confidence=$confidence;
set spark.sql.aqp.behavior=$behavior;
```


###Using WITH ERROR `<fraction> `[CONFIDENCE` <fraction>`] [BEHAVIOR `<string>]` Clause###
* **WITH ERROR** - this is a mandatory clause. The values are  0 < value(double) < 1 . 
* **CONFIDENCE** - this is optional clause. The values are confidence 0 < value(double) < 1 . The default value is 0.95
* **BEHAVIOR** - this is an optional clause. The values are `do_nothing`, `local_omit`, `strict`,  `run_on_full_table`, `partial_run_on_base_table`. The default value is `run_on_full_table`	

For example: 
```
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc with error 0.10 confidence 0.95 behavior ‘local_omit’
```

###DataFrame API###
```
def withError(error: Double,
confidence: Double = Constant.DEFAULT_CONFIDENCE,
behavior: String = "DO_NOTHING"): DataFrame
```

For example:
``` 
snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).orderBy( desc("Month_")).withError(0.10, 0.95, 'local_omit’) 
```

###High-level Accuracy Contracts (HAC)###
SnappyData combines state-of-the-art approximate query processing techniques and a variety of data synopses to ensure interactive analytics over both, streaming and stored data. Using high-level accuracy contracts (HAC), SnappyData offers end users intuitive means for expressing their accuracy requirements, without overwhelming them with statistical concepts.

When an error requirement is not met, the action to be taken is defined in the behavior clause. Approximate queries have HAC support using the following behavior clause. 

**Behaviour Clause** | **Description**
-|-
`<do_nothing>`|The SDE engine returns the estimate as is. 
`<local_omit>`|For aggregates that do not satisfy the error criteria, the value is replaced by a special value like "null". 
`<strict>`|If any of the aggregate column in any of the rows do not meet the HAC requirement, the system throws an exception. 
`<run_on_full_table>`|If any of the single output row exceeds the specified error, then the query is re-executed on the base table.
`<partial_run_on_base_table>`|If the error is more than what is specified in the query, the query is re-executed on the base table for those sub-groups.  This result is then merged with the result derived from the sample table. 

In the following example, any one of the above behavior clause can be applied. 

```
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_  with error <fraction> [CONFIDENCE <fraction>] [BEHAVIOR <behavior>]
```

###Error Functions###
In addition to this, SnappyData supports error functions that can be specified in the query projection. These error functions are supported for the SUM, AVG and COUNT aggregates in the projection. 

The following four methods are available to be used in query projection when running approximate queries:

* **absolute_error(column alias**) : Indicates absolute error present in the estimate (approx answer) calulated using error estimation method (ClosedForm or Bootstrap) 

* **relative_error(column alias)** : Indicates ratio of absolute error to estimate.

* **lower_bound(column alias)** : Lower value of a estimate interval for a given confidence.

* **upper_bound(column alias)**: Upper value of a estimate interval for a given confidence.

Confidence is the probability that the value of a parameter falls within a specified range of values.
For Example:

```
SELECT avg(ArrDelay) as AvgArr ,absolute_error(AvgArr),relative_error(AvgArr),lower_bound(AvgArr), upper_bound(AvgArr),
UniqueCarrier FROM airline GROUP BY UniqueCarrier order by UniqueCarrier WITH ERROR 0.12 confidence 0.9
```
The `absolute_error` and `relative_error` function values returns 0 if query is executed on the base table. 
`lower_bound` and `upper_bound` values returns null if query is executed on the base table.

<!--
### Synopsis Data Engine Technique 1: Sampling
The basic idea behind sampling is the assumption that a representative sample of the base data set can be built such that it can provide answers to aggregate questions like SUM, AVG and COUNT fairly accurately and much more quicker than running the same query against the full data set. We use a combination of techniques to build the sample such that it is representative, random (is not biased) and contains under represented groups.

The two techniques that the SnappyData SDE module uses to accomplish this are reservoir sampling as applied to stratified sampling. 

Reservoir sampling is a technique/set of algorithms for randomly choosing a sample of *k* items from a set S containing n items, where k is a small subset of n. While reservoir sampling delivers a uniform random sample, by itself, it does not have the ability to ensure that under represented groups in the data set are represented in the sample.

This is where stratified sampling comes in. Stratified sampling divides the population/data set into different non overlapping subgroups. Once the strata has been defined, we then use reservoir sampling within each subgroup to deliver uniform random samples. This works for large static data sets or streaming data sets. The process of stratification is driven by prior knowledge of the query column sets that are expected in user queries.


*The following DDL creates a sample that is 3% of the full data set and stratified on 3 columns* 

	CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE OPTIONS(
    buckets '5',
    qcs 'UniqueCarrier, Year_, Month_',
    fraction '0.03',
    strataReservoirSize '50') AS (
    SELECT Year_, Month_ , DayOfMonth,
      DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
      UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
      CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
      Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
      Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
      LateAircraftDelay, ArrDelaySlot
    FROM AIRLINE);
    
   
*Equivalent Scala API for creating  the same sample table*  	 

```scala
    String baseTable = "AIRLINE"
       // Create a sample table sampling parameters.
      snc.createSampleTable(sampleTable, Some(baseTable),
        Map("buckets" -> "5",
          "qcs" -> "UniqueCarrier, Year_, Month_",
          "fraction" -> "0.03",
          "strataReservoirSize" -> "50"
        ))
        snc.table(baseTable).write.insertInto(sampleTable)
```

Here is an example of a query that can be run after the sample table has been created.  

	SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order
        by Month_  with error 0.10 confidence 0.95
	  
Note how the query specifies the acceptable error fraction and expected confidence interval. The table specified in the query is the base table, however the SnappyData SDE engine figures out that there are one or more appropriate sample tables that can be used to satisfy this query and transparently uses the sample table to satisfy the query.  

Here is the scala API for running the same query     

	snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).withError(0.10, 0.95)  
	  
The withError method takes in both the error fraction and the expected confidence interval for the returned result.

In addition to this, SnappyData supports error functions that can be specified in the query projection. Currently these error functions are supported for the SUM and AVG aggregates in the projection. The following four methods are available to be used in query projection when running approximate queries, and their definitions are self explanatory

1. absolute_error(\<Aggregate field used in query>)
2. relative_error(\<Aggregate field used in query>)
3. lower_bound(\<Aggregate field used in query>)
4. upper_bound(\<Aggregate field used in query>)

The query below depicts an example of using error functions in query projections 
 
````
select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay), absolute_error(arrivalDelay),
Year_ from airline group by Year_ order by Year_ with error 0.10 confidence 0.95;
````
Some of the error rates on queries can be high enough to render the query result meaningless. To deal with this, SnappyData offers the ability to set a configuration parameter that governs whether the query fails when the error rate condition cannot be met or whether it should still return the results in such conditions. In the future we expect to change this behavior to allow the user to further specify whether the query should be transparently run against the full data set if available.
-->


## Synopsis Data Engine Technique 2: Synopses##
Synopses data structures are typically much smaller than the base data sets that they represent. They use very little space and provide fast, approximate answers to queries. A [BloomFilter](https://en.wikipedia.org/wiki/Bloom_filter) is a commonly used example of a synopsis data structure. Another example of a synopsis structure is a [Count-Min-Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) which serves as a frequency table of events in a stream of data. The ability to use Time as a dimension for querying makes synopses structures very interesting. As streams are ingested, all relevant synopses are updated incrementally and can be queried using SQL or the Scala API.

### Creating TopK tables###
TopK queries are used to rank attributes to answer "best, most interesting, most important" class of questions. TopK structures store elements ranking them based on their relevance to the query. [TopK](http://stevehanov.ca/blog/index.php?id=122) queries aim to retrieve, from a potentially very large resultset, only the *k (k >= 1)* best answers.
 
*SQL API for creating a TopK table in SnappyData* 
 
```  
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets')")
```  
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables.

*Scala API for creating a TopK table*  
   
	
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
  
*Querying the TopK table*  
	
	
	select * from topkTweets order by EstimatedValue desc  
	
The example above queries the TopK table which returns the top 40 (the depth of the TopK table was set to 40) hashtags with the most retweets.
### Approximate TopK analytics for time series data
Time is used as an attribute in creating the TopK structures. Time can be an attribute of the incoming data set (which is frequently the case with streaming data sets) and in the absence of that, the system uses arrival time of the batch as the timestamp for that incoming batch. The TopK structure is populated along the dimension of time. As an example, the most retweeted hashtags in each window are stored in the data structure. This allows us to issue queries like, "what are the most popular hashtags in a given time interval?" Queries of this nature are typically difficult to execute and not easy to optimize (due to space considerations) in a traditional system.

Here is an example of a time based query on the TopK structure which returns the most popular hashtags in the time interval queried. The SnappyData SDE module provides two attributes startTime and endTime which can be used to run queries on arbitrary time intervals.
	
	
	select hashtag, EstimatedValue, ErrorBoundsInfo from MostPopularTweets where 
        startTime='2016-01-26 10:07:26.121' and endTime='2016-01-26 11:14:06.121' 
        order by EstimatedValue desc
	  
	
If time is an attribute in the incoming data set, it can be used instead of the system generated time. In order to do this, the TopK table creation is provided the name of the column containing the timestamp.

*SQL API for creating a TopK table in SnappyData specifying timestampColumn* 

In the example below tweetTime is a field in the incoming dataset which carries the timestamp of the tweet.
 
```scala
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets', timeSeriesColumn 'tweetTime' )")
```  
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables

*Scala API for creating a TopK table*  

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
Synopsis data engine offers the potential for order of magnitude improvements in big data query processing but it is by no means a panacea to all big data queries. The use of SDE is predicated on proper strata selection for sample generation and that in turn is a function of the queries that the system is expected to handle. Using regions or states as strata for queries involving customers offers the potential for providing manageable subgroups (50 states) with the potential for enough sample data in each subgroup to allow sampling to work. Using customer id as the strata in the same scenario would simply not be feasible. 

In the current release SDE queries only work for SUM, AVG and COUNT aggregations not involving joins. The SnappyData SDE module will gradually expand the scope of queries that can be serviced through it. But the overarching goal here is to make enough of a dent in query processing by diverting at least some queries to the sampling subsystem and allowing better data exploration. 