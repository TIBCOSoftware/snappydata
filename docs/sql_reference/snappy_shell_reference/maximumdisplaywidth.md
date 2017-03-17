#MaximumDisplayWidth

Sets the largest display width for columns to the specified value.

##Syntax

``` pre
MAXIMUMDISPLAYWIDTH integer_value
```

<a id="rtoolsijcomref12281__section_DD09D6819E43465D8822B8619EB6DB6C"></a>
##Description

Sets the largest display width for columns to the specified value. You generally use this command to increase the default value in order to display large blocks of text.

##Example

``` pre
snappy(PEERCLIENT)> maximumdisplaywidth 4;
snappy(PEERCLIENT)> VALUES 'NOW IS THE TIME!';
1
----
NOW&

1 row selected
snappy(PEERCLIENT)> maximumdisplaywidth 30;
snappy(PEERCLIENT)> VALUES 'NOW IS THE TIME!';
1
----------------
NOW IS THE TIME!

1 row selected
```


