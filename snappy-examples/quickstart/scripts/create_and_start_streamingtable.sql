---------------------------------------------------------------------------------------------------------------------
---- Initialize streaming context with 2 second window duration, create stream table and start streaming context ----
---------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS HASHTAGTABLE ;
DROP TABLE IF EXISTS RETWEETTABLE ;
DROP TABLE IF EXISTS HASHTAG_FILESTREAMTABLE ;
DROP TABLE IF EXISTS RETWEET_FILESTREAMTABLE ;

STREAMING INIT 2;

CREATE STREAM TABLE HASHTAGTABLE (hashtag string) USING twitter_stream OPTIONS (consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', 
    consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', 
    accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', 
    accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', 
    rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow') ;

CREATE STREAM TABLE RETWEETTABLE (retweetId long,retweetCnt int, retweetTxt string) USING twitter_stream OPTIONS (consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi',
    consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', 
    accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', 
    accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', 
    rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow') ;


CREATE STREAM TABLE HASHTAG_FILESTREAMTABLE (hashtag string) USING file_stream OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', 
    rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow', 
    directory '/tmp/copiedtwitterdata');

CREATE STREAM TABLE RETWEET_FILESTREAMTABLE (retweetId long, retweetCnt int, retweetTxt string) USING file_stream OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2',
    rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow', 
    directory '/tmp/copiedtwitterdata');

STREAMING START ;
