---------------------------------------------------------------------
---- Initialize streaming context with 2 second window duration, 
---- create file stream table and start streaming context ----
---------------------------------------------------------------------

DROP TABLE IF EXISTS HASHTAGTABLE ;
DROP TABLE IF EXISTS RETWEETTABLE ;

STREAMING INIT 2;

CREATE STREAM TABLE HASHTAGTABLE 
      (hashtag string) 
    USING twitter_stream 
    OPTIONS (consumerKey '***REMOVED***', 
      consumerSecret '***REMOVED***', 
      accessToken '***REMOVED***', 
      accessTokenSecret '***REMOVED***', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow') ;

CREATE STREAM TABLE RETWEETTABLE 
      (retweetId long,retweetCnt int, retweetTxt string) 
    USING twitter_stream 
    OPTIONS (consumerKey '***REMOVED***',
      consumerSecret '***REMOVED***', 
      accessToken '***REMOVED***', 
      accessTokenSecret '***REMOVED***', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow') ;

STREAMING START ;
