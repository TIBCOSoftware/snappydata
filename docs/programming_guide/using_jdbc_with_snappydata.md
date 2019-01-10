# Using JDBC with SnappyData
SnappyData is shipped with few JDBC drivers. The connection URL typically points to one of the locators. In the background, the driver acquires the endpoints for all the servers in the cluster along with load information and automatically connects clients to one of the data servers directly. The driver provides HA by automatically adjusting underlying physical connections in case the servers fail. 

```pre
// 1527 is the default port a Locator or Server uses to listen for thin client connections
Connection c = DriverManager.getConnection ("jdbc:snappydata://locatorHostName:1527/");
// While, clients typically just point to a locator, you could also directly point the 
//   connection at a server endpoint
```

!!! Note
	If you are using a third part tool that connects to the database using JDBC, and if the tool does not automatically select a driver class, you may have the option of selecting a class from within the JAR file. In this case, select the **io.snappydata.jdbc.ClientDriver** class.
    
For more information, see [How to connect using JDBC driver](/howto/connect_using_jdbc_driver.md).
