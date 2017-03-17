# SYS.CHANGE_PASSWORD

Changes the password of an existing non-system BUILTIN user.

##Syntax
------

``` pre
SYS.CHANGE_PASSWORD (
IN USER_ID VARCHAR(128)
IN OLDPASSWORD VARCHAR(128)
IN NEWPASSWORD VARCHAR(128)
)
```

**USER\_ID  **
The name of the existing user. See <a href="show_users.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref" title="Displays a list of all BUILTIN users that are configured in the RowStore member.">SYS.SHOW\_USERS</a>. Note that built-in system users are defined when you boot RowStore members, and you cannot change those passwords except by restarting members; see <a href="../../deploy_guide/Topics/security/builtin-users.html#concept_mrq_1ql_z4" class="xref" title="Built-in system users are defined when you boot the RowStore locator. Other RowStore members that join the system must specify one of the same system users that are defined in the locator. If you need to change the password of a system user, you must stop all members of the distributed system, and then restart them (starting with the locator), specifying the new username definition when you start.">Changing a System User Password</a>.

**OLDPASSWORD **
The existing password of the specified USER\_ID, or a null or empty string for the old password (if an Admin user is changing the password of another user).

A user can change their own password by providing the correct, old password. Admin users can change any password by providing null or an empty string for the old password. If a non-empty value is provided, then it must match the old password.

The ability to change other users' passwords is enabled for the database owner by default. Other users can be explicitly GRANTed EXECUTE permission on the procedure. However, this works only when <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes__section_98DEF23ED88A4821BF4CA852CBB5633A" class="xref noPageCitation">snappydata.sql-authorization</a> is true.

**NEWPASSWORD  **
The new password to assign to the user.

##Example
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


