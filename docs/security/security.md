# Managing Security

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

In the current release, SnappyData only supports LDAP authentication which allows users to authenticate against an existing LDAP directory service in your organization. LDAP (lightweight directory access protocol) provides an open directory access protocol that runs over TCP/IP. </br>This feature provides a secure way for users to use their existing login credentials (usernames and passwords) to access the cluster and data.

SnappyData uses mutual authentication between the SnappyData locator and subsequent SnappyData members that boot and join the distributed system. 

!!! Note:
	
	* Currently, only LDAP based authentication and authorization is supported

	* The user launching the cluster becomes the admin user of the cluster.

	* All members of the cluster (leads, locators, and servers) must be started by the same user

<!--	* The Snappy cluster and the Spark cluster (smart connector mode) must be secure-->
Refer to [User Names for Authentication, Authorization, and Membership](#user-names) for more information on how user names are treated by each system.

## Launching the Cluster in Secure Mode

SnappyData uses mutual authentication between the SnappyData locator and subsequent SnappyData members that boot and join the distributed system.

To enable LDAP authentication, set the following authentication properties in the [configuration files](../configuring_cluster/configuring_cluster.md) **conf/locators**, **conf/servers**, and **conf/leads** files.

* `auth-provider`: The authentication provider. Set the `auth-provider` property to `LDAP`, to enable LDAP for authenticating all distributed system members as well as clients to the distributed system. 

* `user`: The user name of the administrator starting the cluster

* `password`: The password of the administrator starting the cluster

* `J-Dgemfirexd.auth-ldap-server`: Set this property to the URL to the LDAP server.

* `J-Dgemfirexd.auth-ldap-search-base`: Use this property to limit the search space used when SnappyData verifies a user login ID. Specify the name of the context or object to search, that is a parameter to `javax.naming.directory.DirContext.search()`. 

* `J-Dgemfirexd.auth-ldap-search-dn`: If the LDAP server does not allow anonymous binding (or if this functionality is disabled), specify the user distinguished name (DN) to use for binding to the LDAP server for searching.

* `J-Dgemfirexd.auth-ldap-search-pw`: The password for the guest user DN, used for looking up the DN (see `Dgemfirexd.auth-ldap-search-dn`). 

**Example**: 

In the below example, we are launching the locator in secure mode, which communicates with the LDAP server at localhost listening on port 389.
```
localhost -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
          -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-pw=user123
```

!!! Note: 
	You must specify `gemfirexd.auth-ldap-*` properties as Java system properties by prefixing '-J-D'.
    
	If you use SSL-encrypted LDAP and your LDAP server certificate is not recognized by a valid Certificate Authority (CA), you must create a local trust store for each SnappyData member and import the LDAP server certificate to the trust store. See the document on [Creating a Keystore]( http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore) for more information.

	Specify the javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword system properties when you start individual SnappyData members. For example:

		localhost -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
			-J-Dgemfirexd.auth-ldap-server=ldaps://ldapserver:636/ -user=user_name -password=user_pwd \
     			-J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          		-J-Dgemfirexd.auth-ldap-search-pw=user123
          		-J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
			-J-Djavax.net.ssl.trustStore=/Users/user1/snappydata/keystore_name \
			-J-Djavax.net.ssl.trustStorePassword=keystore_password

	javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword must be specified as Java system properties (using the -J option on the Snappy shell).
    
## Authentication - Connecting to a Secure Cluster
Authentication is the process of verifying someone's identity. When a user tries to log in, that request is forwarded to the specified LDAP directory to verify if the credentials are correct.
There are a few different ways to connect to a secure cluster using either JDBC (Thin) Client, Smart Connector Mode and Snappy Jobs. Accessing a secure cluster requires users to provide their user credentials.

### Using JDBC (Thin) Client

When using the JDBC client, provide the user credentials using connection properties 'user' and 'password'.

**Example: JDBC Client**
```
val props = new Properties()
props.setProperty("user", username);
props.setProperty("password", password);

val url: String = s"jdbc:snappydata://localhost:1527/"
val conn = DriverManager.getConnection(url, props)
```

**Example: Snappy shell**

```
connect client 'localhost:1527;user=user1;password=user123';
```

### Using ODBC Driver

You can also connect to the SnappyData Cluster using SnappyData ODBC Driver using the following command:

```
Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>
```

For more information refer to, [How to Connect using ODBC Driver](../howto.md#how-to-connect-using-odbc-driver).

### Using Smart Connector Mode 

In Smart Connector mode, provide the user credentials as Spark configuration properties named `spark.snappydata.store.user` and `spark.snappydata.store.password`.

**Example**</br> 
In the below example, these properties are set in the `SparkConf` which is used to create `SnappyContext` in your job.

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

**Example**</br> 
The below example demonstrates how to connect to the cluster via Spark shell using the `--conf` option to specify the properties.

```
$ bin/spark-shell  
    --master local[*] 
    --conf spark.snappydata.connection=localhost:1527 
    --conf spark.snappydata.store.user=user1
    --conf spark.snappydata.store.password=user123
```

**Example**:</br>
Alternatively, you can specify the user credentials in the Spark conf file. </br> Spark reads these properties when you lauch the spark-shell or invoke spark-submit. 
To do so, specify the user credentials in the **spark-defaults.conf** file, located in the **conf** directory.

In this file, you can specify:
``` bash
spark.snappydata.store.user     <username>
spark.snappydata.store.password <password>
```

### Using Snappy Jobs

When submitting Snappy jobs, using `snappy-job.sh`, provide user credentials through a configuration file using the option `--passfile`. 

For example, a sample configuration file is provided below: 

```
$ cat /home/user1/snappy/job.config 
-u user1:password
```

In the below example, the above configuration file is passed when submitting a job.
```
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

	* These user credentials are passed to the Snappy instance available in the job, and it will be used to authorize the operations in the job.

##  Authorization
Authorization is the process of determining what access permissions the authenticated user has. Users are authorized to perform tasks based on their role assignments. SnappyData also supports LDAP group authorization.

The administrator can manage user permissions in a secure cluster using the [GRANT](../reference/sql_reference/grant.md) and [REVOKE](../reference/sql_reference/revoke.md) SQL statements which allow you to set permission for the user for specific database objects or for specific SQL actions. 

The [GRANT](../reference/sql_reference/grant.md) statement is used to grant specific permissions to users. The [REVOKE](../reference/sql_reference/revoke.md) statement is used to revoke permissions.

!!!Note:

	* A user requiring [INSERT](../reference/sql_reference/insert.md), [UPDATE](../reference/sql_reference/update.md) or [DELETE](../reference/sql_reference/delete.md) permissions may also require explicit [SELECT](../reference/sql_reference/select.md) permission on a table
	
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

The token "%GROUP%" is replaced by the actual group name in the search pattern. A custom search filter should use the same as a placeholder, for the group name. The default member attribute list is member, uniqueMember. The LDAP group resolution is recursive, meaning a group can refer to another group (see example below). There is no detection for broken LDAP group definitions having a cycle of group references and such a situation leads to a failure in GRANT or REFRESH_LDAP_GROUP with StackOverflowError.

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

## Configuring Network Encryption and Authentication using SSL

The network communication between the server and client can be encrypted using SSL. For more information refer to [SSL Setup for Client-Server](../configuring_cluster/ssl_setup.md).

<a id="user-names"></a>

## User Names for Authentication, Authorization, and Membership

### User Names are Converted to Authorization Identifiers
User names in the SnappyData system are known as authorization identifiers. The authorization identifier is a string that represents the name of the user if one was provided in the connection request. 

After the authorization identifier is passed to the SnappyData system, it becomes an SQL92Identifier. SQL92Identifier is a kind of identifier that represents a database object such as a table or column. A SQL92Identifier is case-insensitive (it is converted to all caps) unless it is delimited with double quotes. A SQL92Identifier is limited to 128 characters and has other limitations.

All user names must be valid authorization identifiers even if user authentication is turned off, and even if all users are allowed access to all databases.

### Handling Case Sensitivity and Special Characters in User Names
If an external authentication system is used, SnappyData does not convert a user's name to an authorization identifier until after authentication has occurred (but before the user is authorized). For example, with an example user named Fred:

Within the user authentication system, Fred might be known as FRed. If the external user authentication service is case-sensitive, Fred must always be typed as:
```
connect client 'localhost:1527;user=FRed;password=flintstone';
```
Within the SnappyData user authorization system, Fred becomes a case-insensitive authorization identifier. Here, FRed is known as FRED.

Also consider a second example, where Fred has a slightly different name within the user authentication system:

Within the user authentication system, Fred is known as Fred. You must now put double quotes around the username, because it is not a valid SQL92Identifier. SnappyData removes the double quotes when passing the name to the external authentication system.

```
connect client 'localhost:1527;user="Fred!";password=flintstone';
```

Within the SnappyData user authorization system, Fred now becomes a case-sensitive authorization identifier. In this case, Fred is known as Fred.

As shown in the first example, the external authentication system may be case-sensitive, whereas the authorization identifier within SnappyData may not be. If your authentication system allows two distinct users whose names differ by case, delimit all user names within the connection request to make all user names case-sensitive within the SnappyData system. In addition, you must also delimit user names that do not conform to SQL92Identifier rules with double quotes.