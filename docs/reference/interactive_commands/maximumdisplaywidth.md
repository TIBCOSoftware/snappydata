# MaximumDisplayWidth

Sets the largest display width for columns to the specified value.

## Syntax

```pre
MAXIMUMDISPLAYWIDTH integer_value
```

## Description

Sets the largest display width for columns to the specified value. You generally use this command to increase the default value in order to display large blocks of text.

## Example

```pre
snappy> insert into airlineref values('A-1', 'NOW IS THE TIME');
1 row inserted/updated/deleted
snappy> maximumdisplaywidth 4;
snappy> select * from AIRLINEREF where code='A-1';
CODE|DES&
---------
A-1 |NOW&

1 row selected
snappy>  maximumdisplaywidth 30;
snappy> select * from AIRLINEREF where code='A-1';
CODE |DESCRIPTION                   
------------------------------------
A-1  |NOW IS THE TIME               

1 row selected
```


