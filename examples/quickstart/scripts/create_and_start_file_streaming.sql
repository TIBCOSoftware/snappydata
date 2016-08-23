---------------------------------------------------------------------
---- Initialize streaming context with 2 second window duration, 
---- create file stream table and start streaming context ----
---------------------------------------------------------------------

DROP TABLE IF EXISTS filestream_topktable ;
DROP TABLE IF EXISTS hashtag_filestreamtable ;
DROP TABLE IF EXISTS retweet_filestreamtable ;

STREAMING INIT 2 second;

CREATE STREAM TABLE hashtag_filestreamtable
      (hashtag STRING)
    USING file_stream
    OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow', 
      directory '/tmp/copiedtwitterdata');

CREATE STREAM TABLE retweet_filestreamtable
      (retweetId LONG, retweetCnt INT, retweetTxt STRING)
    USING file_stream
    OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2',
      rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow', 
      directory '/tmp/copiedtwitterdata');

CREATE TOPK TABLE filestream_topktable ON hashtag_filestreamtable OPTIONS
(key 'hashtag', timeInterval '2000ms', size '10' );

STREAMING START;
