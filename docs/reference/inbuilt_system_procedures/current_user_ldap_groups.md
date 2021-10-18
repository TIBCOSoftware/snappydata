# CURRENT\_USER\_LDAP\_GROUPS

The CURRENT\_USER\_LDAP\_GROUPS function returns all the ldap groups (as an ARRAY) of the user who is executing the current SQL statement.

## Example

``` pre
snappy> SELECT array_contains(CURRENT_USER_LDAP_GROUPS(), 'GROUP1');
----------------------------
true

1 row selected
```

**Also see:**

* [Built-in System Procedures and Built-in Functions](index.md)
