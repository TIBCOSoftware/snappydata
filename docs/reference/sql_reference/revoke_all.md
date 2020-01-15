# REVOKE ALL

## Syntax

```pre
REVOKE ALL ON <external-table> FROM <user>;
```

<a id="description"></a>
## Description

The REVOKE ALL statement removes permissions from a specific user on external Tables.

## Examples

Here is an example SQL to revoke permissions to individual users on external tables:

```pre
REVOKE ALL ON EXT_T1 FROM samb,bob;
```
