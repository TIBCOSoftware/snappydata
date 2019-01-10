# disconnect

Disconnects from the database.

## Syntax

```pre
DISCONNECT [ ALL | CURRENT | ConnectionIdentifier ]
```

## Description

Disconnects from the database. Specifically, issues a `java.sql.Connection.close` request against the connection indicated on the command line. There must be a current connection at the time the request is made.

If ALL is specified, all known connections are closed and there will be no current connection.

Disconnect CURRENT is the same as Disconnect without indicating a connection; the default connection is closed.

If a connection name is specified with an identifier, the command disconnects the named connection. The name must be the name of a connection in the current session provided with the [Connect](connect.md) command.

If the [Connect](connect.md) command without the AS clause was used, you can supply the name the system generated for the connection. If the current connection is the named connection when the command completes, there will be no current connection and you must issue a [Connect](connect.md) command.

## Example

``` pre
snappy> DISCONNECT CONNECTION1;
```


