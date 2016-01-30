## Approximate Query processing (AQP)
### Overview
Data volumes from transactional and non transactional sources have been growing exponentially. Cleansing, transforming and storing such large data sets present significant challenges in itself. Querying large data sets in real time using optimizations like columnar storage, compression and horizontal scaling, while necessary, are simply linear improvements to an exponential problem and discourages practitioners from exploring large data sets through querying, because query response times are very large. 

Approximate query processing offers an exponential solution to the data volume problem. The basic idea behind approximate query processing is that one can use statistical sampling techniques and probabilistic data structures to answer aggregate class queries without needing to store or operate over the entire data set. The approach trades off query accuracy for quicker response times, allowing for queries to be run on large data sets with meaningful and accurate error information. A real world example here would be the use of political polls run by Gallup and others where a small sample is used to estimate support for a candidate within a small margin of error. 

Its important to note that not all SQL queries can be answered through AQP, but by moving a subset of queries hitting the database to the AQP module, the system as a whole becomes more responsive and usable.
### Approximations Technique 1: Synopses
Synopses data structures are typically much smaller than the base data sets that they represent. They use very little space and provide fast, approximate answers to queries. A [BloomFilter](https://en.wikipedia.org/wiki/Bloom_filter) is a commonly used example of a synopsis data structure. Another example of a synopsis structure is a [Count-Min-Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) which serves as a frequency table of events in a stream of data. The ability to use Time as a dimension for querying makes synopses structures very interesting. As streams are ingested, all relevant synopses are updated incrementally and can be queried using SQL or the Scala API.

#### Creating TopK tables
TopK queries are used to rank attributes to answer "best, most interesting, most important" class of questions.TopK structures store elements ranking them based on their relevance to the query. [TopK](http://stevehanov.ca/blog/index.php?id=122) queries aim to retrieve, from a potentially very large resultset, only the *k (k >= 1)* best answers  
 
*SQL API for creating a TopK table in SnappyData* 
 
```  
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets')")
```  
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables

*Scala API for creating a TopK table*  
   
	
	val topKOptionMap = Map(
		"epoch" -> System.currentTimeMillis().toString,
        "timeInterval" -> "1000ms",
        "size" -> "40",
            "frequencyCol" -> "retweets",
        "basetable" -> "tweetStreamTable"

      )
      val schema = StructType(List(StructField("HashTag", StringType)))
      snc.createApproxTSTopK("MostPopularTweets", "HashTag",
        schema, topKOptionMap)
	  
The code above shows how to do the same thing using the SnappyData Scala API  
  
*Querying the TopK table*  
	
	
	select * from topkTweets order by EstimatedValue desc  
	
The example above queries the TopK table which returns the top 40 (the depth of the TopK table was set to 40) hashtags with the most retweets.
### Approximate TopK analytics for time series data
Time is used as an attribute in creating the TopK structures. Time can be an attribute of the incoming data set (which is frequently the case with streaming data sets) and in the absence of that, the system uses arrival time of the batch as the timestamp for that incoming batch. The TopK structure is populated along the dimension of time. As an example, the most retweeted hashtags in each window are stored in the data structure. This allows us to issue queries like, "what are the most popular hashtags in a given time interval?" Queries of this nature are typically difficult to execute and not easy to optimize (due to space considerations)in a traditional system.

Here is an example of a time based query on the TopK structure which returns the most popular hashtags in the time interval queried. The SnappyData AQP module provides two attributes startTime and endTime which can be used to run queries on arbitrary time intervals.
	
	
	select hashtag, EstimatedValue, ErrorBoundsInfo from MostPopularTweets where 
        startTime='2016-01-26 10:07:26.121' and endTime='2016-01-26 11:14:06.121' 
        order by EstimatedValue desc
	  
	
If time is an attribute in the incoming data set, it can be used instead of the system generated time. In order to do this, the TopK table creation is provided the name of the column containing the time stamp. 
*SQL API for creating a TopK table in SnappyData specifying timestampColumn* 
 In the example below tweetTime is a field in the incoming dataset which carries the timestamp of the tweet.
 
```  
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets', timeSeriesColumn 'tweetTime' )")
```  
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables

*Scala API for creating a TopK table*  
   
	val topKOptionMap = Map(
		"epoch" -> System.currentTimeMillis().toString,
        "timeInterval" -> "1000ms",
        "size" -> "40",
            "frequencyCol" -> "retweets",
            "timeSeriesColumn" -> "tweetTime"
        "basetable" -> "tweetStreamTable"

      )
      val schema = StructType(List(StructField("HashTag", StringType)))
      snc.createApproxTSTopK("MostPopularTweets", "HashTag",
        schema, topKOptionMap)
	  
The code above shows how to do the same thing using the SnappyData Scala API  

It is worth noting that the user has the ability to disable time as a dimension if desired. This is done by not providing the *timeInterval* attribute when creating the TopK table.


### Approximations Technique 2: Sampling
The basic idea behind sampling is the assumption that a representative sample of the base data set can be built such that it can provide answers to aggregate questions like SUM, AVG and COUNT fairly accurately and much more quicker than running the same query against the full data set. We use a combination of techniques to build the sample such that it is representative, random (is not biased) and contains under represented groups.

The two techniques that the SnappyData AQP module uses to accomplish this are reservoir sampling as applied to stratified sampling. 

Reservoir sampling is a technique/set of algorithms for randomly choosing a sample of *k* items from a set S containing n items, where n is a small subset of n. While reservoir sampling delivers a uniform random sample, by itself, it does not have the ability to ensure that under represented groups in the data set are represented in the sample.

This is where stratified sampling comes in. Stratified sampling divides the population/data set into different non overlapping subgroups. Once the strata has been defined, we then use reservoir sampling within each subgroup to deliver uniform random samples.This works for large static data sets or streaming data sets. The process of stratification is driven by apriori knowledge of the query column sets that are expected in user queries.


*The following DDL creates a sample that is 3% of the full data set and stratified on 3 columns* 

	CREATE SAMPLE TABLE AIRLINE_SAMPLE OPTIONS(
    buckets '5',
    qcs 'UniqueCarrier, Year_, Month_',
    fraction '0.03',
    strataReservoirSize '50',
    basetable 'Airline') AS (
    SELECT Year_, Month_ , DayOfMonth,
      DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
      UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
      CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
      Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
      Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
      LateAircraftDelay, ArrDelaySlot
    FROM AIRLINE);
    
   
*Equivalent Scala API for creating  the same sample table*  	 
	
	 String baseTable = "AIRLINE"		
       // Create a sample table sampling parameters.
      snc.createSampleTable(sampleTable, None,
        Map("buckets" -> "5",
          "qcs" -> "UniqueCarrier, Year_, Month_",
          "fraction" -> "0.03",
          "strataReservoirSize" -> "50",
          "basetable" -> baseTable
        ))
        snc.table(baseTable).write.insertInto(sampleTable)
     
Here is an example of a query that can be run after the sample table has been created.  

	SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order
        by Month_  with error 0.10 confidence 0.95
	  
Note how the query specifies the acceptable error fraction and expected confidence interval. The table specified in the query is the base table, however the SnappyData AQP engine figures out that there are one or more appropriate sample tables that can be used to satisfy this query and transparently uses the sample table to satisfy the query.  

Here is the scala API for running the same query     

	snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).withError(0.10, 0.95)  
	  
The withError method takes in both the error fraction and the expected confidence interval for the returned result.

In additiion to this, SnappyData supports error functions that can be specified in the query projection. Currently these error functions are supported for the SUM and AVG aggregates in the projection. The following four methods are available to be used in query projection when running approximate queries, and their definitions are self explanatory

1. absolute_error(\<Aggregate field used in query>)
2. relative_error(\<Aggregate field used in query>)
3. lower_bound(\<Aggregate field used in query>)
4. upper_bound(\<Aggregate field used in query>)

The query below depicts an example of using error functions in query projections 
 
````
select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay), absolute_error(arrivalDelay),
Year_ from airline group by Year_ order by Year_ with error 0.10 confidence 0.95;
````
Some of the error rates on queries can be high enough to render the query result meaningless. To deal with this, SnappyData offers the ability to set a configuration parameter that governs whether the query fails when the error rate condition cannot be met or whether it should still return the results in such conditions. In the future we expect to change this behavior to allow the user to further specifiy whether the query should be transparently run against the full data set if available.

#### Using AQP	
Approximate query processing offers the potential for order of magnitude improvements in big data query processing but it is by no means a panacea to all big data queries. The use of AQP is predicated on proper strata selection for sample generation and that in turn is a function of the queries that the system is expected to handle. Using regions or states as strata for queries involving customers offers the potential for providing manageable subgroups (50 states) with the potential for enough sample data in each subgroup to allow sampling to work. Using customer id as the strata in the same scenario would simply not be feasible. 

In the current release AQP queries only work for SUM, AVG and COUNT aggregations not involving joins. The SnappyData AQP module will gradually expand the scope of queries that can be serviced through it. But the overarching goal here is to make enough of a dent in query processing by diverting at least some queries to the sampling subsystem and allowing better data exploration. 


