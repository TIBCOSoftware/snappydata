## Using SnappyData Shell
The SnappyData SQL Shell (_snappy_) provides a simple command line interface to the SnappyData cluster. 
It allows you to run interactive queries on row and column stores, run administrative operations and run status commands on the cluster. 
Internally, it uses JDBC to interact with the cluster. You can also use tools like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster) to interact with SnappyData.
```
<!--using javascript as the code language here... should this be sql?-->
javascript

// from the SnappyData base directory  
$ cd quickstart/scripts  
$ ../../bin/snappy  
Version 2.0-BETA
snappy> 

//Connect to the cluster as a client  
snappy> connect client 'localhost:1527'; //It connects to the locator.

//Show active connections  
snappy> show connections;

//Display cluster members by querying a system table  
snappy> select id, kind, status, host, port from sys.members;

//or
snappy> show members;

//Run a sql script. This particular script creates and loads a column table in the default schema  
snappy> run 'create_and_load_column_table.sql';

//Run a sql script. This particular script creates and loads a row table in the default schema  
snappy> run 'create_and_load_row_table.sql';
```

The complete list of commands available through _snappy_shell_ can be found [here](http://gemfirexd.docs.pivotal.io/docs-gemfirexd/reference/gfxd_commands/gfxd-launcher.html)
