---------------------------------------------------------------------
---- Initialize streaming context with 2 second window duration, 
---- create file stream table and start streaming context ----
---------------------------------------------------------------------

DROP TABLE IF EXISTS topktable ;
DROP TABLE IF EXISTS hashtagtable ;
DROP TABLE IF EXISTS retweettable ;

STREAMING INIT 2secs;
-- Provide twitter credentials (consumerKey,consumerSecret,accessToken,accessTokenSecret)
-- in create stream table hashtagtable and retweettable.

CREATE STREAM TABLE hashtagtable
      (hashtag STRING)
    USING twitter_stream
    OPTIONS (consumerKey 'consumerKey',
      consumerSecret 'consumerSecret',
      accessToken 'accessToken',
      accessTokenSecret 'accessTokenSecret',
      rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow') ;

CREATE STREAM TABLE retweettable
      (retweetId LONG, retweetCnt INT, retweetTxt STRING)
    USING twitter_stream
    OPTIONS (consumerKey 'consumerKey',
      consumerSecret 'consumerSecret',
      accessToken 'accessToken',
      accessTokenSecret 'accessTokenSecret',
      rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow') ;

CREATE TOPK TABLE topktable ON hashtagtable OPTIONS
(key 'hashtag', timeInterval '2000ms', size '10' );

STREAMING START ;
