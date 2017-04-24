# autocommit

Turns the connection's auto-commit mode on or off.

##Syntax

``` pre
AUTOCOMMIT { ON | OFF }
```

<a id="rtoolsijcomref25753__section_BCEE08A5AD03414C8CC7BFC46C88AF96"></a>
##Description

Turns the connection's auto-commit mode on or off. JDBC specifies that the default auto-commit mode is `ON`. Certain types of processing require that auto-commit mode be `OFF`.

If auto-commit mode is changed from off to on when a transaction is outstanding, that work is committed when the current transaction commits, not at the time auto-commit is turned on. Use [Commit](commit.md) or [Rollback](rollback.md) before turning on auto-commit when there is a transaction outstanding, so that all prior work is completed before the return to auto-commit mode.

##Example

``` pre
snappy(PEERCLIENT)> AUTOCOMMIT off;
snappy(PEERCLIENT)> insert into airlines VALUES ('NA', 'New Airline', 0.20, 0.07, 0.6, 1.7, 20, 10, 5);
1 row inserted/updated/deleted
snappy(PEERCLIENT)> commit;
```


