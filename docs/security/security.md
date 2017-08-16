# Managing Security

## Launching the Cluster in Secure Mode

In the current release, SnappyData only supports LDAP authentication which allows users to authenticate against an existing LDAP directory service in your organization. LDAP (lightweight directory access protocol) provides an open directory access protocol that runs over TCP/IP. </br>This feature provides a quick and secure way for users to use their existing login credentials (usernames and passwords) to access the cluster and data.

!!! Note:
	
	* Currently, only LDAP based authentication and authorization is supported

	* The user launching the cluster becomes the admin user of the cluster.

	* All members of the cluster (leads, locators, and servers) must be started by the same user

<!--	* The Snappy cluster and the Spark cluster (smart connector mode) must be secure-->

Authentication is the process of verifying someone's identity. When a user tries to log in, that request is forwarded to the specified LDAP directory to verify if the credentials are correct.

To enable LDAP authentication, set the following LDAP authentication properties in the [configuration files](../configuring_cluster/configuring_cluster.md) **conf/locators**, **conf/servers**, and **conf/leads** files.

* `auth-provider`: The authentication provider. When you set the `auth-provider` property to `LDAP`, SnappyData uses LDAP for authenticating all distributed system members as well as clients to the distributed system. Therefore, you must supply the user and password properties at startup. <!--If you omit a value for the password property when starting a SnappyData member, then the member prompts you for a password at the command line.-->

* `user`: The user name of the administrator starting the cluster

* `password`: The password of the administrator starting the cluster

* `J-Dgemfirexd.auth-ldap-server`: Set this property to the URL to the LDAP server.

* `J-Dgemfirexd.auth-ldap-search-base`: Use this property to limit the search space used when SnappyData verifies a user login ID. Specify the name of the context or object to search, that is a parameter to `javax.naming.directory.DirContext.search()`. 

* `J-Dgemfirexd.auth-ldap-search-dn`: If the LDAP server does not allow anonymous binding (or if this functionality is disabled), specify the user distinguished name (DN) to use for binding to the LDAP server for searching.

* `J-Dgemfirexd.auth-ldap-search-pw`: The password for the guest user DN, used for looking up the DN (see `Dgemfirexd.auth-ldap-search-dn`). 

**Example**: 
In the below example, we connect to LDAP server at localhost listening on port 389.
```
localhost -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
          -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-pw=user123
```

!!! Note: 
	You must specify `.auth-ldap-` properties as Java system properties.

##  Authorization
Authorization is the process of determining what access permissions the authenticated user has. Users are authorized to perform tasks based on their role assignments. SnappyData also supports LDAP group authorization.

The administrator can manage user permissions in a secure cluster using the [GRANT](../reference/sql_reference/grant.md) and [REVOKE](../reference/sql_reference/revoke.md) SQL statements which allow you to set permission for the user for specific database objects or for specific SQL actions. 

The [GRANT](../reference/sql_reference/grant.md) statement is used to grant specific permissions to users. The [REVOKE](../reference/sql_reference/revoke.md) statement is used to revoke permissions.

!!!Note:

	* A user requiring [UPDATE](../reference/sql_reference/update.md) or [DELETE](../reference/sql_reference/delete.md) permissions may also require explicit [SELECT](../reference/sql_reference/select.md) permission on a table
	
	* Only the administrator or users with the required permissions can execute built-in procedures (like INSTALL-JAR)

### LDAP Groups in SnappyData Authorization
SnappyData extends the SQL GRANT statement to support LDAP Group names as Grantees.

Here is an example SQL to grant privileges to individual users:
```
GRANT SELECT ON TABLE t TO sam,bob;
```

You can also grant privileges to LDAP groups using the following syntax:

```
GRANT SELECT ON Table t TO ldapGroup:<groupName>, bob;
GRANT INSERT ON Table t TO ldapGroup:<groupName>, bob;
```

SnappyData fetches the current list of members for the LDAP Group and grants each member privileges individually (stored in SnappyData). </br>
Similarly, when a REVOKE SQL statement is executed SnappyData removes the privileges individually for all members that make up a group. To support changes to Group membership within the LDAP Server, there is an additional System procedure to refresh the privileges recorded in SnappyData.

```
CALL SYS.REFRESH_LDAP_GROUP('<GROUP NAME>');
```

This step has to be performed manually by admin when relevant LDAP groups change on the server.

To optimize searching for groups in the LDAP server the following optional properties can be specified. These are similar to the current ones used for authentication: `gemfirexd.auth-ldap-search-base` and `gemfirexd.auth-ldap-search-filter`. The support for LDAP groups requires using LDAP as also the authentication mechanism.

```
gemfirexd.group-ldap-search-base
// base to identify objects of type group
gemfirexd.group-ldap-search-filter
// any additional search filter for groups
gemfirexd.group-ldap-member-attributes
//attributes specifying the list of members
```

If no `gemfirexd.group-ldap-search-base` property has been provided then the one used for authentication gemfirexd.auth-ldap-search-base is used. </br>
If no search filter is specified then SnappyData uses the standard objectClass groupOfMembers (rfc2307) or groupOfNames with attribute as member, or objectClass groupOfUniqueMembers with attribute as uniqueMember.
To be precise, the default search filter is:

```
(&(|(objectClass=group)(objectClass=groupOfNames)(objectClass=groupOfMembers)
  (objectClass=groupOfUniqueNames))(|(cn=%GROUP%)(name=%GROUP%)))
```

The token "%GROUP%" is replaced by the actual group name in the search pattern. A custom search filter should use the same as a placeholder, for the group name. The default member attribute list is: member,uniqueMember. The LDAP group resolution is recursive, meaning a group can refer to another group (see example below). There is no detection for broken LDAP group definitions having a cycle of group references and such a situation leads to a failure in GRANT or REFRESH_LDAP_GROUP with StackOverflowError.

An LDAP group entry can look like below:

```
dn: cn=group1,ou=group,dc=example,dc=com
objectClass: groupOfNames
cn: group1
gidNumber: 1001
member: uid=user1,ou=group,dc=example,dc=com
member: uid=user2,ou=group,dc=example,dc=com
member: cn=group11,ou=group,dc=example,dc=com
```

!!! NOTE:

	* There is NO multi-group support for users yet, so if a user has been granted access by two LDAP groups only the first one will take effect.

	* If a user belongs to LDAP group as well as granted permissions separately as a user, then the latter is given precedence. So even if LDAP group permission is later revoked (or user is removed from LDAP group), the user will continue to have permissions unless explicitly revoked as a user.

	* LDAPGROUP is now a reserved word, so cannot be used for a user name.


## Connecting to a Secure Cluster
There are a few different ways to connect to a secure cluster using either JDBC/Thin Client, Smart Connector Mode and Snappy Jobs. Accessing a secure cluster requires users to provide their user credentials.

### Using JDBC/Thin Client

When using the JDBC/Thin client, provide the user credentials using properties 'user' and 'password': 

```
connect client 'localhost:1527;user=user1;password=user123';
```

### Using Smart Connector Mode 

In Smart Connector mode, provide the user credentials as Spark configuration properties named `spark.snappydata.store.user` and `spark.snappydata.store.password`.

**Example**: 
In the below example, these properties are set in the `SparkConf` which is used to create `SnappyContext`.

```
val conf = new SparkConf()
    .setAppName("My Spark Application with SnappyData")
    .setMaster(s"spark://$hostName:7077")
    .set("spark.executor.cores", TestUtils.defaultCores.toString)
    .set("spark.executor.extraClassPath",
      getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
    .set("snappydata.connection", snappydataLocatorURL)
    .set("spark.snappydata.store.user", username)
    .set("spark.snappydata.store.password", password)
val sc = SparkContext.getOrCreate(conf)
val snc = SnappyContext(sc)
```

The below example demonstrates how to connect to the cluster via Spark shell using the `--conf` option to specify the properties. 

```
$ bin/spark-shell  
    --master local[*] 
    --conf spark.snappydata.connection=localhost:1527 
    --conf spark.snappydata.store.user=user1
    --conf spark.snappydata.store.password=user123
```

### Using Snappy Jobs

When submitting Snappy jobs, using `snappy-job.sh`, provide user credentials through a configuration file using the option `--passfile`. 

For example: 

```
$ cat /home/user1/snappy/job.config 
-u user1:password
$ bin/snappy-job.sh submit  \
    --lead hostNameOfLead:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar \
    --passfile /home/user1/snappy/job.config
```
!!! Note:

	* Only trusted users should be allowed to submit jobs, as an untrusted user may be able to do harm through jobs by invoking internal APIs which can bypass the authorization checks. 
	
	* Currently, SparkJobServer UI may not be accessible when security is enabled, but you can use the `snappy-job.sh` script to access any information required using commands like `status`, `listcontexts`, etc. </br> Execute `./bin/snappy-job.sh` for more details.

	* The configuration file should be in a secure location with read access only to an authorized user.

