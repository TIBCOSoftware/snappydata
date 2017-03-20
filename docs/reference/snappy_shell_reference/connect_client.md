# connect client

Using the JDBC RowStore thin client driver, connects to a RowStore member indicated by the *host:port* values.

##Syntax

``` pre
CONNECT CLIENT 'host:port[;property=value]*' [ AS connectionName ]
```

<a id="reference_85E77D6BF8C949D6BBAABE485FA02FF2__section_A5670CE37F4B40F8ADAC0FC8CA77B9E0"></a>
##Description

Uses the JDBC RowStore thin client driver to connect to a RowStore member indicated by the *host:port* values. You can specify an optional name for your connection. Use the <a href="set_connection.html#rtoolsijcomref39198" class="xref" title="Specifies which connection to make current when more than one connection is open.">set connection</a> to switch between multiple connections. If you do not name a connection, the system generates a name automatically.

If the connection requires a user name and password, supply those with the optional properties.

If the connect succeeds, the connection becomes the current one and `snappy-shell` displays a new prompt for the next command to be entered. If you have more than one open connection, the name of the connection appears in the prompt.

All further commands are processed against the new, current connection.

##Example

``` pre
snappy-shell version 1.4.0
snappy> connect client 'localhost:1527' as clientConnection;
snappy> show connections;
CLIENTCONNECTION* -     jdbc:snappydata://localhost:1527/
* = current connection
```


