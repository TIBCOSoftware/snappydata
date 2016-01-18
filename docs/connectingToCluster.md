## Getting Started with SnappyData SQL Shell

The SnappyData SQL Shell (_snappy-shell_) provides a simple command line interface to the SnappyData cluster. It allows you to run interactive queries on row and column stores, run administrative operations and run status commands on the cluster. Internally it uses JDBC to interact with the cluster. You can also use tools like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster) to interact with SnappyData.

```
sql  
// from the SnappyData base directory  
**$ cd quickstart/scripts  
$ ../../bin/snappy-shell  
Version 2.0-SNAPSHOT.1  
snappy> **

//Connect to the cluster as a client  
snappy> connect client 'localhost:1527';

//Show active connections  
snappy> show connections;

//Display cluster members by querying a system table  
snappy> select id, kind, status, host, port from sys.members;

//Run a sql script. This particular script creates and loads a column table in the default schema  
snappy> run 'create_and_load_column_table.sql';


//Run a sql script. This particular script creates and loads a row table in the default schema  
snappy> run 'create_and_load_row_table.sql';

//Start pulse to monitor the SnappyData cluster  
snappy> start pulse;
```

The complete list of commands available through _snappy_shell_ can be found [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-launcher.html)


## Using JDBC with SnappyData

SnappyData System supports two different JDBC drivers to connect to the cluster. These allow two different ways of connecting into the cluster. The SnappyData cluster is a peer to peer connected cluster where group members are connected to each other. The embedded mode driver is part of an application that connects into the cluster as a peer. The client mode driver is typically an application that connects into the cluster as a client (TCP connection over a network)

###Embedded mode driver
When an application initiates a connection to SnappyData using the peer JDBC driver, it starts a SnappyData peer member that joins other peers in the same cluster. 

If your application embeds a SnappyData member of any type (a peer client, server, or locator), you should use the ServiceManager API to embed the required type. For example to Join the already running cluster as Accessor(this is a SnappyData member that does not host data, but otherwise participates in the cluster) 

```
val props:Properties = new Properties()
// add desired properties if required.
props.setProperty("locators", "localhost[10334]")
props.setProperty("host-data", "false")
val server :ServiceManager = ServiceManager.getServerInstance()
server.start(props)

```

If you just need a connection object in the embedded mode the following code can be used and it will start the Snappy server on it's own.  

```
val props:Properties = new Properties()
// add desired properties if required.
props.setProperty("locators", "localhost[10334]")
props.setProperty("host-data", "false")
//to get Connection to this server
DriverManager.getConnection("jdbc:snappydata:", props);
```


###Thin Client Driver for Non Embedded Mode
The thin client driver class is packaged in com.pivotal.gemfirexd.jdbc.ClientDriver. In addition to the basic JDBC Connection URL, you specify the host and port number of a Snappy server or a Snappy locator to which the thin client driver will connect. 

```
try {
// 1527 is the default port that a Snappy server uses to listen for thin client connections
  val conn = DriverManager.getConnection ("jdbc:snappydata://myHostName:1527/")
// do something with the connection
}
catch {
  case sqlException: SQLException =>
  println ("SQLException: " + ex.getMessage () + "SQLState: " + ex.getSQLState () )
}
```







