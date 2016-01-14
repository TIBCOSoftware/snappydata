## Getting Started with SnappyData SQL Shell

The SnappyData SQL Shell (_snappy-shell_) provides a simple way to inspect the catalog,  run admin operations,  manage the schema and run interactive queries. You can also use your favorite SQL tool like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster)

```sql
// from the SnappyData base directory
$ cd quickstart/scripts
$ ../../bin/snappy-shell
Version 2.0-SNAPSHOT.1
snappy> 

--Connect to the cluster using thin client connection  ..
snappy> connect client 'localhost:1527';

--Check all the available connections ...
snappy> show connections; 

--To list each cluster member and its status in the cluster ...
snappy> select id, kind, status, host, port from sys.members;

-- Use quickstart script to create column table and load data ...
snappy> run 'create_and_load_column_table.sql';


-- Use quickstart script to create row table and load data ...
snappy> run 'create_and_load_row_table.sql';

--Start pulse to monitor cluster
snappy> start pulse;
```

You can learn more about SnappyData SQL Shell comands [here] http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-launcher.html



## Connecting to SnappyData from JDBC

SnappyData System supports two different JDBC drivers to connect to the cluster.

###Peer JDBC Driver for Embedded Mode
The embedded, peer-to-peer distributed system (also known as an embedded cluster) is the building block for all SnappyData installations. With an embedded cluster, the core SnappyData engine is embedded alongside an existing Java application in the same JVM. When the application initiates a connection to SnappyData using the peer JDBC driver, it starts a SnappyData peer member that joins other peers in the same cluster. 

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







