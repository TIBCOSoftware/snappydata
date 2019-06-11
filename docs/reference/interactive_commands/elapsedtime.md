# elapsedtime

Displays the total time elapsed during statement execution.

## Syntax

```pre
ELAPSEDTIME { ON | OFF }
```

## Description

When `elapsedtime` is turned on, `snappy` displays the total time elapsed during statement execution. The default value is OFF.

## Example

```pre
snappy> elapsedtime on;
snappy> select * from airlines;
A&|AIRLINE_FULL            |BASIC_RATE            |DISTANCE_DISCOUNT     |BUSINESS_LEVEL_FACTOR |FIRSTCLASS_LEVEL_FACT&|ECONOMY_SE&|BUSINESS_S&|FIRSTCLASS&
-----------------------------------------------------------------------------------------------------------------------------------------------------------
NA|New Airline             |0.2                   |0.07                  |0.6                   |1.7                   |20         |10         |5
US|Union Standard Airlines |0.19                  |0.05                  |0.4                   |1.6                   |20         |10         |5
AA|Amazonian Airways       |0.18                  |0.03                  |0.5                   |1.5                   |20         |10         |5

3 rows selected
ELAPSED TIME = 2 milliseconds
```


