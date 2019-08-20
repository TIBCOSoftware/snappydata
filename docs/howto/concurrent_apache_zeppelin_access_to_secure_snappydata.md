<a id="howto-consurrent-zeppelin"></a>
# How to Configure Apache Zeppelin to Securely and Concurrently access the SnappyData Cluster

Multiple users can concurrently access a secure SnappyData cluster by configuring the JDBC interpreter setting in Apache Zeppelin. The JDBC interpreter allows you to create a JDBC connection to a SnappyData cluster.

!!! Note

	* Currently, only the `%jdbc` interpreter is supported with a secure SnappyData cluster.

	* Each user accessing the secure SnappyData cluster should configure the `%jdbc` interpreter in Apache Zeppelin as described in this section.

## Step 1: Download, Install and Configure SnappyData
1. [Download and install SnappyData Enterprise Edition](../install.md) </br>

2. [Configure the SnappyData cluster with security enabled](../security/security.md).

3. [Start the SnappyData cluster](start_snappy_cluster.md).

	- Create a table and load data.

	- Grant the required permissions for the users accessing the table.

        For example:

            snappy> GRANT SELECT ON Table airline TO user2;
        	snappy> GRANT INSERT ON Table airline TO user3;
        	snappy> GRANT UPDATE ON Table airline TO user4;

	!!! Note
    	User requiring INSERT, UPDATE or DELETE permissions also require explicit SELECT permission on a table.

5. Extract the contents of the Zeppelin binary package. </br> 

6. Start the Zeppelin daemon using the command: </br> `./bin/zeppelin-daemon.sh start`

## Configure the JDBC Interpreter
Log on to Zeppelin from your web browser and configure the [JDBC Interpreter](https://zeppelin.apache.org/docs/0.7.0/interpreter/jdbc.html).

		Zeppelin web server is started on port 8080
		http://<IP address>:8080/#/

## Configure the Interpreter

1. Log on to Zeppelin from your web browser and select **Interpreter** from the **Settings** option.

2. Edit the existing `%jdbc` interpreter and configure the interpreter properties.
	The table lists the properties required for SnappyData:
    
    | Property | Value |Description|
	|--------|--------|--------|
	|default.url|jdbc:snappydata://localhost:1527/|Specify the JDBC URL for SnappyData cluster in the format `jdbc:snappydata://<locator_hostname>:1527`|
    |default.driver|io.snappydata.jdbc.ClientDriver|Specify the JDBC driver for SnappyData|
    |default.password|user123|The JDBC user password|
    |default.user|user1|The JDBC username|

3. **Dependency settings**</br> Since Zeppelin includes only PostgreSQL driver jar by default, you need to add the Client (JDBC) JAR file path for SnappyData. The SnappyData Client (JDBC) JAR file (snappydata-jdbc-2.11_1.1.1.jar) is available on [the release page](https://github.com/SnappyDataInc/snappydata/releases/latest). </br>
	The SnappyData Client (JDBC) JAR file (snappydata-jdbc_2.11-1.1.1.jar)can also be placed under **<ZEPPELIN_HOME>/interpreter/jdbc** before starting Zeppelin instead of providing it in the dependency setting.

4. If required, edit other properties, and then click **Save** to apply your changes. 

**See also**

*  [How to Use Apache Zeppelin with SnappyData](use_apache_zeppelin_with_snappydata.md)
*  [How to connect using JDBC driver](/howto/connect_using_jdbc_driver.md)
