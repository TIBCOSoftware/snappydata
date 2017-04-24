# SYS.CHANGE_PASSWORD

Changes the password of an existing non-system BUILTIN user.

## Syntax
------

``` pre
SYS.CHANGE_PASSWORD (
IN USER_ID VARCHAR(128)
IN OLDPASSWORD VARCHAR(128)
IN NEWPASSWORD VARCHAR(128)
)
```

**USER\_ID  **
The name of the existing user. See <mark> TO BE CONFIRMED RowStore link[SYS.SHOW_USERS]() </mark>. Note that built-in system users are defined when you boot SnappyData members, and you cannot change those passwords except by restarting members; see <mark> TO BE CONFIRMED RowStore link [Changing a System User Password]</mark>.

**OLDPASSWORD **
The existing password of the specified USER\_ID, or a null or empty string for the old password (if an Admin user is changing the password of another user).

A user can change their own password by providing the correct, old password. Admin users can change any password by providing null or an empty string for the old password. If a non-empty value is provided, then it must match the old password.

The ability to change other users' passwords is enabled for the database owner by default. Other users can be explicitly GRANTed EXECUTE permission on the procedure. However, this works only when [snappydata.sql-authorization](../../reference/configuration_parameters/snappydata.sql-authorization.md) is true.

**NEWPASSWORD  **
The new password to assign to the user.

## Example
-------

Change the password of a specified user:

``` pre
snappy> call sys.change_password('SIMON','oldpasword','newpassword');
Statement executed.
```

Change the password of a specified user, using the database owner account:

``` pre
snappy> call sys.change_password('SIMON',null,'newpassword');
Statement executed.
```


