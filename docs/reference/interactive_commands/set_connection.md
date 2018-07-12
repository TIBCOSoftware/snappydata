# set connection

Specifies which connection to make current when more than one connection is open.

## Syntax

``` pre
SET CONNECTION Identifier;
```

## Description

Allows you to specify which connection to make current when you have more than one connection open. Use the [Show Connections](show.md) command to display open connections.

If there is no such connection, an error results and the current connection is unchanged.

## Example

``` pre
snappy(CONNECTION0)> set connection CONNECTION1;
snappy(CONNECTION1)> show connections;
CONNECTION0 - 	jdbc:snappydata:thrift://127.0.0.1[1527]
CONNECTION1* - 	jdbc:snappydata:thrift://127.0.0.1[1527]
* = current connection
```


