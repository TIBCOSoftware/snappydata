#Configuring SnappyData as a JDBC Datasource
The SnappyData JDBC implementation enables you to use a distributed system as an embedded JDBC datasource in products such as WebLogic Server.

Follow this general procedure when setting up SnappyData as a datasource in a third-party product:

1. Copy the **gemfirexd-client.jar** file to the location where other JDBC drivers for the product are stored.

2. For products such as WebLogic Server that provide datasource templates, select “Apache Derby,” “User-defined,” or “Other” as the database type if SnappyData is not explicitly supported.

3. Specify “snappydata” as the database name. This represents a single SnappyData distributed system. (SnappyData does not contain multiple databases as in Apache Derby or other relational database systems.)

4. For the hostname and port, specify the hostname and port combination of a SnappyData locator or a SnappyData server. This is the same hostname and port combination you would use to connect as a client from the `snappy` prompt.

5. For the database username and password, enter a valid username and password combination if you have enabled authentication in your system (using the `-auth-provider` property).
If you have not configured authentication in SnappyData, specify “app” as both the username and password values, or any other temporary value.

	!!! Note:
		SnappyData uses the username specified in the JDBC connection as the schema name when you do not provide the schema name for a database object. SnappyData uses “APP” as the default schema. If your system does not enable authentication, you can specify “APP” for both the username and password to maintain consistency with the default schema behavior.

6. For the driver class, specify: 

        com.pivotal.snappydata.internal.jdbc.ClientConnectionPoolDataSource <mark>to be confirmed </mark>

7. The JDBC URL that you specify must begin with `jdbc:snappydata://`. Remove any template properties such as `create=true` if they are present in the URL or properties fields.</br>
 In products such as WebLogic, you cannot specify JDBC connection properties for an embedded datasource as part of the JDBC URL. Instead, use the properties field and specify connectionAttributes using the format:
        
        connectionAttributes=attribute;attribute;...
For example:

        connectionAttributes=mcast-address=239.192.81.1;mcast-port=10334 or connectionAttributes=locators=239.192.81.1;mcast-port=0
See also the Apache Derby documentation for [EmbeddedDataSource](http://db.apache.org/derby/docs/10.4/publishedapi/jdbc3/org/apache/derby/jdbc/EmbeddedDataSource.html).
        
8. A process can connect to only one SnappyData distributed system at a time. If you want to connect to a different SnappyData distributed system, shut down the current embedded data source before re-connecting with a different datasource.
