# connect client

## Syntax

```pre
CONNECT CLIENT 'host:port[;property=value]*' [ AS connectionName ]
```

## Description

Uses the JDBC SnappyData thin client driver to connect to a SnappyData member indicated by the *host:port* values. You can specify an optional name for your connection. Use the [set connection](set_connection.md) to switch between multiple connections. If you do not name a connection, the system generates a name automatically.

If the connection requires a user name and password, supply those with the optional properties.

If the connect succeeds, the connection becomes the current one and `snappy` displays a new prompt for the next command to be entered. If you have more than one open connection, the name of the connection appears in the prompt.

All further commands are processed against the new, current connection.

## Example

```pre
SnappyData version 1.0.2.1 
snappy> connect client 'localhost:1527' as clientConnection;
snappy> show connections;
CLIENTCONNECTION* -     jdbc:snappydata:thrift://localhost[1527]
* = current connection
```


