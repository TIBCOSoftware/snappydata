# set connection

Specifies which connection to make current when more than one connection is open.

## Syntax

```no-highlight
SET CONNECTION Identifier
```

## Description

Allows you to specify which connection to make current when you have more than one connection open. Use the [Show Connections](show.md) command to display open connections.

If there is no such connection, an error results and the current connection is unchanged.

## Example

```no-highlight
snappy> set connection clientConnection;
snappy(CLIENTCONNECTION)> show connections;
CLIENTCONNECTION* -     jdbc:snappydata://localhost:1527/
PEERCLIENT -    jdbc:snappydata:
* = current connection
snappy(CLIENTCONNECTION)>
```


