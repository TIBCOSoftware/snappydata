# User Names for Authentication, Authorization, and Membership <a id="user-names"></a>

## User Names are Converted to Authorization Identifiers
User names in the TIBCO ComputeDB system are known as authorization identifiers. The authorization identifier is a string that represents the name of the user if one was provided in the connection request. 

After the authorization identifier is passed to the TIBCO ComputeDB system, it becomes an SQL92Identifier. SQL92Identifier is a kind of identifier that represents a database object such as a table or column. A SQL92Identifier is case-insensitive (it is converted to all caps) unless it is delimited with double quotes. A SQL92Identifier is limited to 128 characters and has other limitations.

All user names must be valid authorization identifiers even if user authentication is turned off, and even if all users are allowed access to all databases.

## Handling Case Sensitivity and Special Characters in User Names
If an external authentication system is used, TIBCO ComputeDB does not convert a user's name to an authorization identifier until after authentication has occurred (but before the user is authorized). For example, with an example user named Fred:

Within the user authentication system, Fred might be known as FRed. If the external user authentication service is case-sensitive, Fred must always be typed as:
```pre
connect client 'localhost:1527;user=FRed;password=flintstone';
```
Within the TIBCO ComputeDB user authorization system, Fred becomes a case-insensitive authorization identifier. Here, FRed is known as FRED.

Also consider a second example, where Fred has a slightly different name within the user authentication system:

Within the user authentication system, Fred is known as Fred. You must now put double quotes around the username, because it is not a valid SQL92Identifier. TIBCO ComputeDB removes the double quotes when passing the name to the external authentication system.

```pre
connect client 'localhost:1527;user="Fred!";password=flintstone';
```

Within the TIBCO ComputeDB user authorization system, Fred now becomes a case-sensitive authorization identifier. In this case, Fred is known as Fred.

As shown in the first example, the external authentication system may be case-sensitive, whereas the authorization identifier within TIBCO ComputeDB may not be. If your authentication system allows two distinct users whose names differ by case, delimit all user names within the connection request to make all user names case-sensitive within the TIBCO ComputeDB system. In addition, you must also delimit user names that do not conform to SQL92Identifier rules with double quotes.
