# connect

Connects to the database indicated by the *ConnectionURLString*.

##Syntax

``` pre
CONNECT ConnectionURLString [ PROTOCOL Identifier ]
    [ AS Identifier ]
```

<a id="rtoolsijcomref22318__section_4397917B333C4918B009F5F6A7DDC742"></a>
##Description

Connects to the database indicated by the *ConnectionURLString*. You have the option of specifying a name for your connection. Use the <a href="set_connection.html#rtoolsijcomref39198" class="xref" title="Specifies which connection to make current when more than one connection is open.">Set Connection</a> command to switch between connections. If you do not name a connection, the system generates a name automatically.

You also have the option of specifying a named protocol previously created with the <a href="protocol.html#rtoolsijcomref27997" class="xref" title="Specifies the protocol, as a String, for establishing connections and automatically loads the appropriate driver.">Protocol</a> command.

<p class="note"><strong>Note:</strong> If the connection requires a user name and password, supply those in the connection URL string, as shown in the example. </p>
If the connect succeeds, the connection becomes the current one and `snappy` displays a new prompt for the next command to be entered. If you have more than one open connection, the name of the connection appears in the prompt.

All further commands are processed against the new, current connection.

##Example

``` pre
snappy> protocol 'jdbc:derby:';
snappy> connect '//armenia:29303/myDB;user=a;password=a' as db5Connection; 
snappy> show connections;
DB5CONNECTION* -        jdbc:derby://armenia:29303/myDB
* = current connection
```


