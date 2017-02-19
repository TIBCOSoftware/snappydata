## Using JDBC with SnappyData
SnappyData is shipped with few JDBC drivers. The connection URL typically points to one of the locators. In the background, the driver acquires the endpoints for all the servers in the cluster along with load information, and automatically connects clients to one of the data servers directly. The driver provides HA by automatically adjusting underlying physical connections in case the servers fail. 

```java

// 1527 is the default port a Locator or Server uses to listen for thin client connections
Connection c = DriverManager.getConnection ("jdbc:snappydata://locatorHostName:1527/");
// While, clients typically just point to a locator, you could also directly point the 
//   connection at a server endpoint
```
!!! Note
	 If the tool does not automatically select a driver class, you may have the option of selecting a class from within the JAR file. In this case, select the **io.snappydata.jdbc.ClientDriver** class.