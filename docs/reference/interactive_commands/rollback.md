#rollback

Issues a *java.sql.Connection.rollback* request.

##Syntax

``` pre
ROLLBACK 
```

<a id="rtoolsijcomref12297__section_897487E737AA4677847C3827C0C87793"></a>
##Description

Issues a *java.sql.Connection.rollback* request. Use only if auto-commit is off. A *java.sql.Connection.rollback* request undoes the currently active transaction and initiates a new transaction.

##Example

``` pre
snappy(localhost:<port number>)> set isolation read committed;
0 rows inserted/updated/deleted
snappy(localhost:<port number>)> values current isolation;
1
----
CS

1 row selected
snappy(localhost:<port number>)> insert into airlines VALUES ('AN', 'Another New Airline', 0.20, 0.07, 0.6, 1.7, 20, 10, 5);
1 row inserted/updated/deleted
snappy(localhost:<port number>)> AUTOCOMMIT off;
snappy(localhost:<port number>)> select * from airlines;
A&|AIRLINE_FULL            |BASIC_RATE            |DISTANCE_DISCOUNT     |BUSINESS_LEVEL_FACTOR |FIRSTCLASS_LEVEL_FACT&|ECONOMY_SE&|BUSINESS_S&|FIRSTCLASS&
-----------------------------------------------------------------------------------------------------------------------------------------------------------
NA|New Airline             |0.2                   |0.07                  |0.6                   |1.7                   |20         |10         |5
US|Union Standard Airlines |0.19                  |0.05                  |0.4                   |1.6                   |20         |10         |5
AA|Amazonian Airways       |0.18                  |0.03                  |0.5                   |1.5                   |20         |10         |5
AN|Another New Airline     |0.2                   |0.07                  |0.6                   |1.7                   |20         |10         |5

4 rows selected
snappy(localhost:<port number>)> rollback;
snappy(localhost:<port number>)> select * from airlines;
A&|AIRLINE_FULL            |BASIC_RATE            |DISTANCE_DISCOUNT     |BUSINESS_LEVEL_FACTOR |FIRSTCLASS_LEVEL_FACT&|ECONOMY_SE&|BUSINESS_S&|FIRSTCLASS&
-----------------------------------------------------------------------------------------------------------------------------------------------------------
NA|New Airline             |0.2                   |0.07                  |0.6                   |1.7                   |20         |10         |5
US|Union Standard Airlines |0.19                  |0.05                  |0.4                   |1.6                   |20         |10         |5
AA|Amazonian Airways       |0.18                  |0.03                  |0.5                   |1.5                   |20         |10         |5

3 rows selected
```


