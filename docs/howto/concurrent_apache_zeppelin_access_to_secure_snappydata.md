<a id="howto-consurrent-zeppelin"></a>
# How to Configure Apache Zeppelin to Securely and Concurrently access the SnappyData Cluster

Multiple users can concurrently access a secure SnappyData cluster by configuring the JDBC interpreter setting in Apache Zeppelin. The JDBC interpreter allows you to create a JDBC connection to a SnappyData cluster.

!!! Note

	* Currently, only the `%jdbc` interpreter is supported with a secure SnappyData cluster.

	* Each user accessing the secure SnappyData cluster should configure the `%jdbc` interpreter in Apache Zeppelin as described here.

## Step 1: Download, Install and Configure SnappyData

1. [Download and install SnappyData](../install/index.md).

2. [Configure the SnappyData cluster with security enabled](../security/index.md).

3. [Start the SnappyData cluster](start_snappy_cluster.md).

    - Create a table and load data.

    - Grant the required permissions for the users accessing the table.

        For example:

``` sql
        snappy> GRANT SELECT ON Table airline TO user2;
        snappy> GRANT INSERT ON Table airline TO user3;
        snappy> GRANT UPDATE ON Table airline TO user4;
```

   To enable running `EXEC SCALA` also `GRANT`:

``` sql
        snappy> GRANT PRIVILEGE EXEC SCALA TO user2;
```


!!! Note

    User requiring INSERT, UPDATE or DELETE permissions also require explicit SELECT permission on a table.

!!! IMPORTANT

    Beware that granting EXEC SCALA privilege is overarching by design and essentially makes the user equivalent to the database adminstrator since scala code can be used to modify any data using internal APIs.

4. Follow the remaining steps as given in [How to Use Apache Zeppelin with SnappyData](use_apache_zeppelin_with_snappydata.md)

**See also**

*  [How to Use Apache Zeppelin with SnappyData](use_apache_zeppelin_with_snappydata.md)
*  [How to connect using JDBC driver](../howto/connect_using_jdbc_driver.md)
