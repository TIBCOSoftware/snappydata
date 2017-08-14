# Managing Security

## Launching the Cluster in Secure Mode

LDAP (lightweight directory access protocol) provides an open directory access protocol that runs over TCP/IP. SnappyData supports LDAP authentication which allows users to authenticate against an existing LDAP directory service in your organization. </br>This feature provides a quick and secure way for users to use their existing login credentials (usernames and passwords) to access the cluster and data.

!!! Note:
	
	* Currently, only LDAP based authentication and authorization is supported.

	* Ensure that user with administrative privileges launches the cluster. 

	* All members of the cluster (leads, locators, and servers) must be started by the same user (with administrative privileges)

## Authentication
Authentication is the process of verifying someone's identity. When a user tries to log in, that request is forwarded to the specified LDAP directory to verify if the credentials are correct.

To enable LDAP authentication, set the following LDAP authentication properties in the [configuration files](../configuring_cluster/configuring_cluster.md) **conf/locators**, **conf/servers**, and **conf/leads** files.

* -auth-provider: The authentication provider. The value should be set to `LDAP`

* -J-Dsnappydata.auth-ldap-server: The location of the LDAP server

* -user: The user name of the administrator starting the cluster

* -password: The password of the administrator starting the cluster

**Example**: 
```
-auth-provider=LDAP 
-J-Dsnappydata.auth-ldap-server=ldap://localhost:389/ 
-user=user1 
-password=user123
```

##  Authorization
Authorization is the process of determining what access permissions the authenticated user has. Users are authorized to perform tasks based on their role assignments. SnappyData also supports LDAP group authorization.

When the LDAP authorization mode is enabled, the [GRANT](../reference/sql_reference/grant.md) and [REVOKE](../reference/sql_reference/revoke.md) SQL statements enables you to set the user's permission for specific database objects or for specific SQL actions. 

The [GRANT](../reference/sql_reference/grant.md) statement is used to grant specific permissions to users. The [REVOKE](../reference/sql_reference/revoke.md) statement is used to revoke permissions.

!!!Note:
	Before you do the [UPDATE](../reference/sql_reference/update.md) operation ensure that you have permissions to perform the [SELECT](../reference/sql_reference/select.md) operation.

## Connecting to a Secure Cluster
There are a few different ways to connect to a secure cluster using either JDBC/Thin Client, Smart Connector Mode and Snappy Jobs.

### Using JDBC/Thin Client

When using the JDBC/Thin client, run the following command in the Snappy shell to connect:

```
connect client 'localhost:1527;user=user1;password=user123';
```

### Using Smart Connector Mode 

When using the Smart Connector Mode, run the following command in the Spark shell to connect:

```
$ bin/spark-shell  
    --master local[*] 
    --conf spark.snappydata.connection=localhost:1527 
    --conf spark.snappydata.store.user=user1
    --conf spark.snappydata.store.password=user123
```

### Using Snappy Jobs

When using SnappyJobs, the following command can be submitted to the job server to connect:

**Parameter**:

* `--passfile /location_of_pass_file/`: Enter the location of the passfile which contains the user authentication credentials in the following format:`-u username:password`.</br> Ensure that this passfile is located at a secure location.

**Example**: 

```
$ bin/snappy-job.sh submit 
    --lead localhost:8090 
    --app-name myapp 
    --class io.snappydata.hydra.security.CreateAndLoadTablesSnappyJob 
    --app-jar /home/user1/snappy/snappydata/dtests/build-artifacts/scala-2.11/libs/snappydata-store-scala-tests-0.1.0-SNAPSHOT-tests.jar 
    --conf dataLocation=/home/user1/snappy/snappydata/examples/quickstart/data/airlineParquetData 
    --passfile /home/user1/snappy/snappydata/dtests/src/resources/scripts/security/user1Credentials.txt
```

