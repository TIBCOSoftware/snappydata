---------------------------------------------------------------------
---- Initialize streaming context with 2 second window duration, 
---- create file stream table and start streaming context ----
---------------------------------------------------------------------

DROP TABLE IF EXISTS HASHTAG_FILESTREAMTABLE ;
DROP TABLE IF EXISTS RETWEET_FILESTREAMTABLE ;

STREAMING INIT 2;

CREATE STREAM TABLE HASHTAG_FILESTREAMTABLE 
      (hashtag string) 
    USING file_stream 
    OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow', 
      directory '/tmp/copiedtwitterdata');

CREATE STREAM TABLE RETWEET_FILESTREAMTABLE 
      (retweetId long, retweetCnt int, retweetTxt string) 
    USING file_stream 
    OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2',
      rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow', 
      directory '/tmp/copiedtwitterdata');

STREAMING START ;
