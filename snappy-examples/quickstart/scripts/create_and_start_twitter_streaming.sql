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
    OPTIONS (consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', 
      consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', 
      accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', 
      accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow') ;

CREATE STREAM TABLE RETWEETTABLE 
      (retweetId long,retweetCnt int, retweetTxt string) 
    USING twitter_stream 
    OPTIONS (consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi',
      consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', 
      accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', 
      accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', 
      rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow') ;

STREAMING START ;
