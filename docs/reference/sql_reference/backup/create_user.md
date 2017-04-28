# SYS.CREATE_USER

Adds a new BUILTIN user account to the SnappyData distributed system.

## Syntax

``` pre
SYS.CREATE_USER (
IN USER_ID VARCHAR(128)
IN PASSWORD VARCHAR(128)
)
```

**USER_ID**   
The name of the user to create. Note that SnappyData stores normalized (all upper-case) user names, so "User1" and "user1" refer to the same BUILTIN user. See also <a href="show_users.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref" title="Displays a list of all BUILTIN users that are configured in the SnappyData member.">SYS.SHOW_USERS</a>.

**NEWPASSWORD**  
The new password to assign to the user. See also <a href="change_password.html#reference_A7533A4A873D48FBAB05A67DD5CC7F66" class="xref" title="Changes the password of an existing non-system BUILTIN user.">SYS.CHANGE_PASSWORD</a>.

##Example
-------

**Create a new BUILTIN user:**

``` pre
snappy> call sys.create_user('SIMON','apassword');
Statement executed.
```


