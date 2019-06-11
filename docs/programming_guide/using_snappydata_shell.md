# Using SnappyData Shell
The SnappyData SQL Shell (_snappy-sql_) provides a simple command line interface to the SnappyData cluster.
It allows you to run interactive queries on the row and column stores, run administrative operations and run status commands on the cluster. 
Internally, it uses JDBC to interact with the cluster. You can also use tools like SquirrelSQL or DBVisualizer( JDBC to connect to the cluster) to interact with SnappyData.

[Start the SnappyData cluster]() and enter the following:

```pre
// From the SnappyData base directory  
$ ./bin/snappy-sql
SnappyData version 1.1.0
snappy-sql> 

//Connect to the cluster as a client  
snappy-sql> connect client 'localhost:1527'; //It connects to the locator which is running in localhost with client port configured as 1527.

//Show active connections  
snappy-sql> show connections;

//Display cluster members by querying a system table  
snappy-sql> select id, kind, status, host, port from sys.members;

//or
snappy-sql> show members;

//Run a sql script. This particular script creates and loads a column table in the default schema  
snappy-sql> run './quickstart/scripts/create_and_load_column_table.sql';

//Run a sql script. This particular script creates and loads a row table in the default schema  
snappy-sql> run './quickstart/scripts/create_and_load_row_table.sql';
```

The complete list of commands available through _snappy_shell_ can be found [here](../reference/command_line_utilities/store-launcher.md).

