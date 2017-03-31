# disconnect

Disconnects from the database.

##Syntax

``` pre
DISCONNECT [ ALL | CURRENT | ConnectionIdentifier ]
```

<a id="rtoolsijcomref20382__section_F79D073A57464B3587FD0F449234776B"></a>
##Description

Disconnects from the database. Specifically issues a `java.sql.Connection.close` request against the connection indicated on the command line. There must be a current connection at the time the request is made.

If ALL is specified, all known connections are closed and there will be no current connection.

Disconnect CURRENT is the same as Disconnect without indicating a connection; the default connection is closed.

If a connection name is specified with an identifier, the command disconnects the named connection. The name must be the name of a connection in the current session provided with the <a href="connect.html#rtoolsijcomref22318" class="xref" title="Connects to the database indicated by the ConnectionURLString.">Connect</a> command.

If the <a href="connect.html#rtoolsijcomref22318" class="xref" title="Connects to the database indicated by the ConnectionURLString.">Connect</a> command without the AS clause was used, you can supply the name the system generated for the connection. If the current connection is the named connection, when the command completes, there will be no current connection and you must issue a <a href="set_connection.html#rtoolsijcomref39198" class="xref" title="Specifies which connection to make current when more than one connection is open.">Set Connection</a> or <a href="connect.html#rtoolsijcomref22318" class="xref" title="Connects to the database indicated by the ConnectionURLString.">Connect</a> command.

##Example


``` pre
snappy(PEERCLIENT)> disconnect peerclient;
snappy>
```


