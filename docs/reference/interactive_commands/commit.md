# commit

Issues a *java.sql.Connection.commit* request.

##Syntax

``` pre
COMMIT
```

<a id="rtoolsijcomref31510__section_8AFE9E666623401FA0F87457575AE267"></a>
##Description

Issues a *java.sql.Connection.commit* request. Use this command only if auto-commit is off. A *java.sql.Connection.commit* request commits the currently active transaction and initiates a new transaction.

##Example

``` pre
snappy(localhost:<port number>)> AUTOCOMMIT off;
snappy(localhost:<port number>)> insert into airlines VALUES ('NA', 'New Airline', 0.20, 0.07, 0.6, 1.7, 20, 10, 5);
1 row inserted/updated/deleted
snappy(localhost:<port number>)> commit;
```


