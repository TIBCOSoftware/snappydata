<a id="howto-snappyShell"></a>
# How to Use Snappy SQL shell (snappy-sql)
The Snappy SQL shell can be used to execute SQL on SnappyData cluster. In the background, `snappy-sql` uses JDBC connections to execute SQL.

**Connect to a SnappyData Cluster**: </br>
Use the `snappy-sql` and `connect client` commands on the Snappy SQL shell as follows:

```pre
$ ./bin/snappy-sql
snappy> connect client '<locatorHostName>:1527';
```

Here, the `<locatorHostName>` is the host name of the node on which the locator is started and **1527** is the default port on which the locator listens for connections.

**Execute SQL queries**:</br> Once connected, you can execute SQL queries using `snappy-sql`

```pre
snappy> CREATE TABLE APP.PARTSUPP (PS_PARTKEY INTEGER NOT NULL PRIMARY KEY, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL) USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY') ;

snappy> INSERT INTO APP.PARTSUPP VALUES(100, 1, 5000, 100);

snappy> INSERT INTO APP.PARTSUPP VALUES(200, 2, 50, 10);

snappy> SELECT * FROM APP.PARTSUPP;
PS_PARTKEY |PS_SUPPKEY |PS_AVAILQTY|PS_SUPPLYCOST    
-----------------------------------------------------
100        |1          |5000       |100.00           
200        |2          |50         |10.00            

2 rows selected
```

**View the members of cluster**: </br>
Use the `show members` command.

```pre
snappy> show members;
ID                            |HOST                          |KIND                          |STATUS              |NETSERVERS                    |SERVERGROUPS                  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
192.168.63.1(21412)<v1>:61964 |192.168.63.1                  |datastore(normal)             |RUNNING             |localhost/127.0.0.1[1528]     |                              
192.168.63.1(21594)<v2>:29474 |192.168.63.1                  |accessor(normal)              |RUNNING             |                              |IMPLICIT_LEADER_SERVERGROUP   
localhost(21262)<v0>:22770    |localhost                     |locator(normal)               |RUNNING             |localhost/127.0.0.1[1527]     |                              

3 rows selected
```

**View the list tables in a schema**: </br>
Use `show tables in <schema>` command.

```pre
snappy> show tables in app;
TABLE_SCHEM         |TABLE_NAME                    |TABLE_TYPE|REMARKS             
-----------------------------------------------------------------------------------
APP                 |PARTSUPP                      |TABLE     |                    

1 row selected
```
