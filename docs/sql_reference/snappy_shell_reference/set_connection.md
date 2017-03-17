#set connection

Specifies which connection to make current when more than one connection is open.

##Syntax

``` pre
SET CONNECTION Identifier
```

<a id="rtoolsijcomref39198__section_74D83416E976409A944698F9E95476E7"></a>
##Description

Allows you to specify which connection to make current when you have more than one connection open. Use the <a href="show.html#rtoolsijcomrefshow" class="xref" title="Displays information about active connections and database objects.">Show Connections</a> command to display open connections.

If there is no such connection, an error results and the current connection is unchanged.

##Example

``` pre
snappy(PEERCLIENT)> set connection clientConnection;
snappy(CLIENTCONNECTION)> show connections;
CLIENTCONNECTION* -     jdbc:snappydata://localhost:1527/
PEERCLIENT -    jdbc:snappydata:
* = current connection
snappy(CLIENTCONNECTION)>
```


