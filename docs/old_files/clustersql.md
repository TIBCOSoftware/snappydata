For SQL, the SnappyData SQL Shell (_snappy-sql_) provides a simple way to inspect the catalog,  run admin operations,  manage the schema and run interactive queries. You can also use your favorite SQL tool like SquirrelSQL or DBVisualizer (a JDBC connection to the cluster).

From the SnappyData base directory, /snappy/, run: 
````
./bin/snappy-sql
````

Connect to the cluster with

````snappy> connect client 'localhost:1527';````

You can view connections with

````snappy> show connections;````

And check member status with:

````snappy> show members;````