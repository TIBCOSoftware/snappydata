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
snappy(localhost:<port number>)>maximumdisplaywidth 4;
snappy(localhost:<port number>)> Insert into AIRLINEREF values('A-1','NOW IS THE TIME');
1 row inserted/updated/deleted
snappy(localhost:<port number>)> Select * from AIRLINEREF where code='A-1';
CODE|DES&
------------------
A-1 |NOW&

snappy(localhost:<port number>)> maximumdisplaywidth 30;
snappy(localhost:<port number>)> Insert into AIRLINEREF values('A-2','NOW IS THE TIME');
1 row inserted/updated/deleted
snappy(localhost:<port number>)>Select * from AIRLINEREF where code='A-2';
CODE           |DESCRIPTION    
-------------------------------
A-2            |NOW IS THE TIME
```


