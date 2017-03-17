# java.sql.Driver Interface

<a id="java-sql-driver__section_D37F20015E0F473C95F4252D4E02A793"></a>
The java.sql.Driver.getPropertyInfo method method returns a *DriverPropertyInfo* object. In a RowStore system, this consists of an array of database connection URL attributes. (See <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes" class="xref" title="You use JDBC connection properties, connection boot properties, and Java system properties to configure RowStore members and connections.">Configuration Properties</a>.) To get the *DriverPropertyInfo* object, request the JDBC driver from the driver manager:

``` pre
String url = "jdbc:snappydata:";
Properties info = new Properties();
java.sql.DriverManager.getDriver(url).getPropertyInfo(url, info);
```


