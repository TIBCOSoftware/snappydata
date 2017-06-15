# log-file

## Description

File to use for writing log messages. If this property is not set, the default is used.

Each member type has its own default output:

-   leader: `snappyleader.log`
-   locator: `snappylocator.log`
-   server: `snappyserver.log`

Use the snappydata. prefix for SnappyData members, or use the snappydata.client prefix for client-side logging.

## Usage
localhost -log-file=/home/supriya/snappy/server/snappy-server.log

## Default Value

not set

## Property Type

connection

## Prefix

snappydata. or snappydata.client.
