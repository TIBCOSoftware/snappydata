# GRANT ALL

## Syntax


```pre
GRANT ALL ON <external-table> to <user>;
```

## Description

This syntax is used to grant the permissions on external tables for other users.


## Example

Here is an example SQL to grant privileges to individual users on external tables:

```pre
GRANT ALL ON EXT_T1 TO samb,bob;
```