# commit

## Syntax

```pre
COMMIT
```

<a id="description"></a>
## Description

Issues a *java.sql.Connection.commit* request. Use this command only if auto-commit is **off**. A *java.sql.Connection.commit* request commits the currently active transaction and initiates a new transaction.

## Example

``` pre
snappy> AUTOCOMMIT off;
snappy> INSERT INTO greetings values (DEFAULT, 'hello');
1 row inserted/updated/deleted
snappy> COMMIT;
```
