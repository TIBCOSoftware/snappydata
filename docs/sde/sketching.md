# Sketching 

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent> 

Synopses data structures are typically much smaller than the base data sets that they represent. They use very little space and provide fast, approximate answers to queries. A [BloomFilter](https://en.wikipedia.org/wiki/Bloom_filter) is a commonly used example of a synopsis data structure. Another example of a synopsis structure is a [Count-Min-Sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) which serves as a frequency table of events in a stream of data. The ability to use Time as a dimension for querying makes synopses structures much more useful. As streams are ingested, all relevant synopses are updated incrementally and can be queried using SQL or the Scala API.

## Creating TopK tables
TopK queries are used to rank attributes to answer "best, most interesting, most important" class of questions. TopK structures store elements ranking them based on their relevance to the query. [TopK](http://stevehanov.ca/blog/index.php?id=122) queries aim to retrieve, from a potentially very large result set, only the *k (k >= 1)* best answers.
 
### SQL API for creating a TopK table in SnappyData
 
```pre
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets')")
``` 
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest retweets value in the base table. This works for both static tables and streaming tables.

### Scala API for creating a TopK table
   
	
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
  
### Querying the TopK table
	
	
	select * from topkTweets order by EstimatedValue desc 
	
The example above queries the TopK table which returns the top 40 (the depth of the TopK table was set to 40) hashtags with the most re-tweets.

## Approximate TopK analytics for time series data
Time is used as an attribute in creating the TopK structures. Time can be an attribute of the incoming data set (which is frequently the case with streaming data sets) and in the absence of that, the system uses arrival time of the batch as the time stamp for that incoming batch. The TopK structure is populated along the dimension of time. As an example, the most re-tweeted hashtags in each window are stored in the data structure. This allows us to issue queries like, "what are the most popular hashtags in a given time interval?" Queries of this nature are typically difficult to execute and not easy to optimize (due to space considerations) in a traditional system.

Here is an example of a time-based query on the TopK structure which returns the most popular hashtags in the time interval queried. The SnappyData SDE module provides two attributes startTime and endTime which can be used to run queries on arbitrary time intervals.
	
	
	select hashtag, EstimatedValue, ErrorBoundsInfo from MostPopularTweets where 
        startTime='2016-01-26 10:07:26.121' and endTime='2016-01-26 11:14:06.121' 
        order by EstimatedValue desc
	  
	
If time is an attribute in the incoming data set, it can be used instead of the system generated time. In order to do this, the TopK table creation is provided the name of the column containing the timestamp.

### SQL API for creating a TopK table in SnappyData specifying timestampColumn

In the example below tweetTime is a field in the incoming dataset which carries the timestamp of the tweet.
 
```pre
snsc.sql("create topK table MostPopularTweets on tweetStreamTable " +
        "options(key 'hashtag', frequencyCol 'retweets', timeSeriesColumn 'tweetTime' )")
``` 
The example above create a TopK table called MostPopularTweets, the base table for which is tweetStreamTable. It uses the hashtag field of tweetStreamTable as its key field and maintains the TopN hashtags that have the highest re-tweets value in the base table. This works for both static tables and streaming tables

### Scala API for creating a TopK table 

```pre
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
