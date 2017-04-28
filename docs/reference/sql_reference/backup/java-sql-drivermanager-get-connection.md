# java.sql.DriverManager.getConnection Method

A Java application using the JDBC API establishes a connection to a distributed system by obtaining a *Connection* object. The standard way to obtain a *Connection* object is to call the method *DriverManager.getConnection*, which takes a String containing a connection URL. A JDBC connection URL (uniform resource locator) identifies a source of data.

*DriverManager.getConnection* can take one argument besides a connection URL, a *Properties* object. You can use the *Properties* object to set connection URL attributes.

You can also supply strings representing user names and passwords. When they are supplied, SnappyData checks whether they are valid for the current system if user authentication is enabled. User names are passed to SnappyData as authorization identifiers, which determine whether the user is authorized for access to the database and determine the default schema. When the connection is established, if no user is supplied, SnappyData sets the default schema to *APP*. If a user is supplied, the default schema is the same as the user name.


##SnappyData Connection URL Syntax

A SnappyData connection URL consists of the connection URL protocol (`jdbc:`) followed by the subprotocol (`gemfirexd:`) and then optional attributes.

<a id="java-sql-drivermanger-get-connection__section_D8B664723C4546CA9EEFA1DA661B795A"></a>

## Syntax of Connection URLs for *peer-clients*

For applications that run in a *peer-client*, the syntax of the connection URL is

``` pre
    *jdbc:snappydata:_[;attributes]*_*
```

[]()`jdbc:gemfirexd`   
In JDBC terminology, *gemfirexd* is the *subprotocol* for connecting to a GemFire distributed system. The subprotocol is always *gemfirexd* and does not vary.

`attributes`   
Specify 0 or more connection URL attributes as detailed in [Configuration Properties](../../reference/configuration_parameters/config_parameters/#configuration-properties).

## Additional SQL Syntax

SnappyData also supports the following SQL standard syntax to obtain a reference to the current connection in a server-side JDBC routine:

``` pre
    *jdbc:default:connection*
```
