# CREATE STREAM TABLE

## SYNTAX

**To Create Stream Table:**

```
// DDL for creating a stream table
CREATE STREAM TABLE [IF NOT EXISTS] table_name
(COLUMN_DEFINITION)
USING 'kafka_stream | file_stream | twitter_stream | socket_stream | directkafka_stream'
OPTIONS (
// multiple stream source specific options
  storagelevel 'string-constant',
  rowConverter 'string-constant',
  topics 'string-constant',
  kafkaParams 'string-constant',
  consumerKey 'string-constant',
  consumerSecret 'string-constant',
  accessToken 'string-constant',
  accessTokenSecret 'string-constant',
  hostname 'string-constant',
  port 'string-constant',
  directory 'string-constant'
)
```

## Description

<mark>
TO BE DONE
</mark>

## Example: 

```
//create a connection
snappy> connect client 'localhost:1527';

// Initialize streaming with batchInterval of 2 seconds
snappy> streaming init 2secs;

// Create a stream table
snappy> create stream table streamTable (id long, text string, fullName string, country string,
        retweets int, hashtag  string) using twitter_stream options (consumerKey '', consumerSecret '',
        accessToken '', accessTokenSecret '', rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter');

// Start the streaming
snappy> streaming start;

//Run ad-hoc queries on the streamTable on current batch of data
snappy> select id, text, fullName from streamTable where text like '%snappy%'

// Drop the streamTable
snappy> drop table streamTable;

// Stop the streaming
snappy> streaming stop;
```



