## Quick start  

This 5 minute tutorial provides a quick introduction to SnappyData. It exposes you to the cluster runtime and running OLAP, OLTP SQL.

The following script starts up a minimal set of essential components to form a SnappyData cluster - A locator, one data server 
and one lead node. All nodes are started on localhost.
The locator is primarily responsible to make all the nodes aware of each other, allows the cluster to expand or shrink dynamically and provides a consistent membership view to each node even in the presence of failures (a distributed system membership service). The Lead node hosts the Spark Context and driver and orchestrates execution of Spark Jobs. 
The Data server is the work horse - manages all in-memory data, OLTP execution engine and Spark executors. 

See the  [‘Getting Started’](..) section for more details. 

From the product install directory run this script ..

````shell
./sbin/snappy-start-all.sh
````
This may take 30 seconds or more to bootstrap the entire cluster on your local machine (logs are in the 'work' sub-directory). 
The output should look something like this …
````
$ sbin/snappy-start-all.sh 
localhost: Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
...
localhost: SnappyData Locator pid: 56703 status: running

localhost: Starting SnappyData Server using locators for peer discovery: jramnara-mbpro[10334]   
       (port used for members to form a p2p cluster)
localhost: SnappyData Server pid: 56819 status: running
localhost:   Distributed system now has 2 members.

localhost: Starting SnappyData Leader using locators for peer discovery: jramnara-mbpro[10334]
localhost: SnappyData Leader pid: 56932 status: running
localhost:   Distributed system now has 3 members.

localhost:   Other members: jramnara-mbpro(56703:locator)<v0>:54414, jramnara-mbpro(56819:datastore)<v1>:39737

````
At this point, the SnappyData cluster is up and running and is ready to accept jobs and SQL requests via JDBC/ODBC.
You can [monitor the Spark cluster at port 4040](http://localhost:4040).

For SQL, the SnappyData SQL Shell `snappy-shell` provides a simple way to inspect the catalog,  run admin operations, 
manage the schema and run interactive queries. 

From product install directory run: 
````
$ ./bin/snappy-shell
````
Now, you are ready to try connecting and running SQL on SnappyData. 
On the `snappy-shell` prompt  …

Connect to the cluster with

````snappy> connect client 'localhost:1527';````

And check member status with:

````snappy> show members;````

Now, lets create one column and one row table and load some data. Simply copy/paste to the shell. 
```sql
snappy> run './quickstart/scripts/create_and_load_column_table.sql';
snappy> run './quickstart/scripts/create_and_load_row_table.sql';
```

Now, you can run analytical queries or execute execute transactions on this data. OLAP queries are automatically executed 
through Spark driver and executors. 

```sql
snappy> run './quickstart/scripts/olap_queries.sql';
```

You can study the memory consumption, query execution plan, etc. from the [Spark console](http://localhost:4040).

Congratulations! 

## Where to go next?

Next, we recommend going through more in-depth examples in [Getting Started](#getting-started). Here you will find more details on the 
concepts and experience SnappyData’s AQP, Stream analytics functionality both using SQL and Spark API.
You can also go through our very preliminary [docs](../) and provide us your comments. 

If you are interested in contributing please visit the [contributor page](contribution) for ways in which you can help. 



