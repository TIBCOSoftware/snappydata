# CREATE STREAM TABLE

**To Create Stream Table:**

```pre
// DDL for creating a stream table
CREATE STREAM TABLE [IF NOT EXISTS] table_name
    ( column-definition	[ , column-definition  ] * )
    USING kafka_stream | file_stream | twitter_stream | socket_stream
    OPTIONS (
    // multiple stream source specific options
      storagelevel 'cache-data-option',
      rowConverter 'rowconverter-class-name',
      subscribe 'comma-seperated-topic-name',
      kafkaParams 'kafka-related-params',
      consumerKey 'consumer-key',
      consumerSecret 'consumer-secret',
      accessToken 'access-token',
      accessTokenSecret 'access-token-secret',
      hostname 'socket-streaming-hostname',
      port 'socket-streaming-port-number',
      directory 'file-streaming-directory'
	)
```

For more information on column-definition, refer to [Column Definition For Column Table](create-table.md#column-definition).

Refer to these sections for more information on [Creating Table](create-table.md), [Creating Sample Table](create-sample-table.md), [Creating External Table](create-external-table.md) and [Creating Temporary Table](create-temporary-table.md).

## Description

Create a stream table using a stream data source. If a table with the same name already exists in the database, an exception will be thrown.

`USING <data source>` </br>
Specify the streaming source to be used for this table.

`storageLevel`</br>
Provides different trade-offs between memory usage and CPU efficiency.

`rowConverter`</br>
Converts the unstructured streaming data to a set of rows.

`topics`</br>
Subscribed Kafka topics.

`kafkaParams`</br>
Kafka configuration parameters such as `metadata.broker.list`, `bootstrap.servers` etc.

`directory`</br>
HDFS directory to monitor for the new file.

`hostname`</br>
Hostname to connect to, for receiving data.

`port`</br>
Port to connect to, for receiving data.

`consumerKey`</br>
Consumer Key (API Key) for your Twitter account.

`consumerSecret`</br>
Consumer Secret key for your Twitter account.

`accessToken`</br>
Access token for your Twitter account.

`accessTokenSecret`</br>
Access token secret for your Twitter account.

!!! Note 
	You need to register to [https://apps.twitter.com/](https://apps.twitter.com/) to get the `consumerKey`, `consumerSecret`, `accessToken` and `accessTokenSecret` credentials.

## Example

```pre
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
snappy> select id, text, fullName from streamTable where text like '%snappy%';

// Drop the streamTable
snappy> drop table streamTable;

// Stop the streaming
snappy> streaming stop;
```



