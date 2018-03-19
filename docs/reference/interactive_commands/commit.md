# commit

## Syntax

```no-highlight
COMMIT
```

<a id="description"></a>
## Description

Issues a *java.sql.Connection.commit* request. Use this command only if auto-commit is **off**. A *java.sql.Connection.commit* request commits the currently active transaction and initiates a new transaction.

## Example

```no-highlight
snappy(PEERCLIENT)> AUTOCOMMIT off;
snappy(PEERCLIENT)> insert into airlines VALUES ('NA', 'New Airline', 0.20, 0.07, 0.6, 1.7, 20, 10, 5);
1 row inserted/updated/deleted
snappy(PEERCLIENT)> commit;
```


