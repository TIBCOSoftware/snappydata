#  Authorization

Authorization is the process of determining what access permissions the authenticated user has. Users are authorized to perform tasks based on their role assignments. TIBCO ComputeDB also supports LDAP group authorization.

The administrator can manage user permissions in a secure cluster using the [GRANT](../reference/sql_reference/grant.md) and [REVOKE](../reference/sql_reference/revoke.md) SQL statements which allow you to set permission for the user for specific database objects or for specific SQL actions. 

The [GRANT](../reference/sql_reference/grant.md) statement is used to grant specific permissions to users. The [REVOKE](../reference/sql_reference/revoke.md) statement is used to revoke permissions.

!!! Note

	* A user requiring [INSERT](../reference/sql_reference/insert.md), [UPDATE](../reference/sql_reference/update.md) or [DELETE](../reference/sql_reference/delete.md) permissions may also require explicit [SELECT](../reference/sql_reference/select.md) permission on a table
	
	* Only administrators can execute built-in procedures (like INSTALL-JAR)

### Adding Restrictions in Default Schema

Users in TIBCO ComputeDB cluster have their own schema by default when they log into the cluster. They have full access within this schema.
But in some cases, cluster administrators may need to ensure controlled use of the cluster resources by its users and may need to enforce restrictions on them.

This can be achieved by setting the system property `snappydata.RESTRICT_TABLE_CREATION` to true in **conf** files at the time of starting the cluster.
This forbids the users to create tables in their default schema. Users also cannot execute queries on tables in the schema.

Administrators, however, can explicitly grant permissions to these users on their respective default schemas using GRANT command. The default value of the property is false.

You need to prefix `-J-D` to the property name while specifying it in the conf files (locators, leads, and servers).
```
$ cat conf/servers
localhost -auth-provider=LDAP -J-Dsnappydata.RESTRICT_TABLE_CREATION=true -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
          -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-pw=user123
```

## LDAP Groups in TIBCO ComputeDB Authorization
TIBCO ComputeDB extends the SQL GRANT statement to support LDAP Group names as Grantees.

Here is an example SQL to grant privileges to individual users on column/row tables:

```pre
GRANT SELECT ON TABLE t TO sam,bob;
```

Here is an example SQL to grant privileges to individual users on external tables:

```pre
GRANT ALL ON EXT_T1 TO samb,bob;
```

You can also grant privileges to LDAP groups using the following syntax:

```pre
GRANT SELECT ON Table t TO ldapGroup:<groupName>, bob;
GRANT INSERT ON Table t TO ldapGroup:<groupName>, bob;
```

You can also grant privileges to LDAP groups using the following syntax on external tables:

```pre
GRANT ALL ON EXT_T1 TO ldapGroup:<groupName>, bob;
```

TIBCO ComputeDB fetches the current list of members for the LDAP Group and grants each member privileges individually (stored in TIBCO ComputeDB). </br>
Similarly, when a REVOKE SQL statement is executed TIBCO ComputeDB removes the privileges individually for all members that make up a group. To support changes to Group membership within the LDAP Server, there is an additional System procedure to refresh the privileges recorded in TIBCO ComputeDB.

```pre
CALL SYS.REFRESH_LDAP_GROUP('<GROUP NAME>');
```

This step has to be performed manually by admin when relevant LDAP groups change on the server.

To optimize searching for groups in the LDAP server the following optional properties can be specified. These are similar to the current ones used for authentication: `gemfirexd.auth-ldap-search-base` and `gemfirexd.auth-ldap-search-filter`. The support for LDAP groups requires using LDAP as also the authentication mechanism.

```pre
gemfirexd.group-ldap-search-base
// base to identify objects of type group
gemfirexd.group-ldap-search-filter
// any additional search filter for groups
gemfirexd.group-ldap-member-attributes
//attributes specifying the list of members
```

If no `gemfirexd.group-ldap-search-base` property has been provided then the one used for authentication `gemfirexd.auth-ldap-search-base` is used. </br>
If no search filter is specified then TIBCO ComputeDB uses the standard objectClass groupOfMembers (rfc2307) or groupOfNames with attribute as member, or objectClass groupOfUniqueMembers with attribute as uniqueMember.
To be precise, the default search filter is:

```pre
(&(|(objectClass=group)(objectClass=groupOfNames)(objectClass=groupOfMembers)
  (objectClass=groupOfUniqueNames))(|(cn=%GROUP%)(name=%GROUP%)))
```

The token "%GROUP%" is replaced by the actual group name in the search pattern. A custom search filter should use the same as a placeholder, for the group name. The default member attribute list is member, uniqueMember. The LDAP group resolution is recursive, meaning a group can refer to another group (see example below). There is no detection for broken LDAP group definitions having a cycle of group references and such a situation leads to a failure in GRANT or REFRESH_LDAP_GROUP with StackOverflowError.

An LDAP group entry can look like below:

```pre
dn: cn=group1,ou=group,dc=example,dc=com
objectClass: groupOfNames
cn: group1
gidNumber: 1001
member: uid=user1,ou=group,dc=example,dc=com
member: uid=user2,ou=group,dc=example,dc=com
member: cn=group11,ou=group,dc=example,dc=com
```

!!! Note

	* There is NO multi-group support for users yet, so if a user has been granted access by two LDAP groups only the first one will take effect.

	* If a user belongs to LDAP group as well as granted permissions separately as a user, then the latter is given precedence. So even if LDAP group permission is later revoked (or user is removed from LDAP group), the user will continue to have permissions unless explicitly revoked as a user.

	* LDAPGROUP is now a reserved word, so cannot be used for a user name.
