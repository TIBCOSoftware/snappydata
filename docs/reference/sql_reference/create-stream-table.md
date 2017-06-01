# CREATE STREAM TABLE

## SYNTAX

**To Create Stream Table:**

```
// DDL for creating a stream table
CREATE STREAM TABLE [IF NOT EXISTS] table_name
    (COLUMN_DEFINITION)
    USING kafka_stream | file_stream | twitter_stream | socket_stream | directkafka_stream
    OPTIONS (
    // multiple stream source specific options
      storagelevel 'cache-data-option',
      rowConverter 'rowconverter-class-name',
      topics 'comma-seperated-topic-name',
      kafkaParams 'kafka-related-params',
      consumerKey 'aws-consumer-key',
      consumerSecret 'aws-consumer-secret',
      accessToken 'aws-access-token',
      accessTokenSecret 'aws-access-token-secret',
      hostname 'socket-streaming-hostname',
      port 'socket-streaming-port-number',
      directory 'file-streaming-directory'
	)
```

## Description

Create a stream table using a steam data source. If a table with the same name already exists in the database, an exception will be thrown.

**STREAM**
    Indicates that a stream table will be created

**IF NOT EXISTS**
    If a table with the same name already exists in the database, nothing will happen.

**USING <data source>**
    Specify the streaming source to be used for this table. 

## Example

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



