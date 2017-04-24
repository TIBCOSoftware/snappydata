#wait for

Displays the results of a previously started asynchronous command.

##Syntax

``` pre
WAIT FOR Identifier
```

<a id="rtoolsijcomref17631__section_BCF1EFCA7579448BB39738DD738ECE7E"></a>
##Description

Displays the results of a previously started asynchronous command.

The identifier for the asynchronous command must have been used in a previous [async](http://rowstore.docs.snappydata.io/docs/reference/store_commands/async.html) command on this connection. The Wait For command waits for the SQL statement to complete execution, if it has not already, and then displays the results. If the statement returns a result set, the Wait For command steps through the rows, not the [async](http://rowstore.docs.snappydata.io/docs/reference/store_commands/async.html) command. This action might result in further execution time passing during the result display.

##Example

See [async](http://rowstore.docs.snappydata.io/docs/reference/store_commands/async.html).


<mark> TO BE CONFIRMED ROWSOTRE LINK</mark>