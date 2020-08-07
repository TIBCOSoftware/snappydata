# How to use Transactions Isolation Levels

TIBCO ComputeDB supports transaction isolation levels when using JDBC or ODBC connections. The default transaction level in TIBCO ComputeDB is set to NONE. This corresponds to the JDBC TRANSACTION_NONE isolation level. At this level writes performed by a single thread are seen by all other threads in the order in which they were issued, but writes from different threads may be seen in a different order by other threads.

TIBCO ComputeDB also supports `READ_COMMITTED` and `REPEATABLE_READ` transaction isolation levels. A detailed description of the transaction's semantics in TIBCO ComputeDB can be found in the [Overview of TIBCO ComputeDB Distributed Transactions](../consistency/transactions_about.md) section.

!!! Note
	If you set the isolation level to `READ_COMMITTED` or `REPEATABLE_READ`, queries on column table report an error if [autocommit](../reference/interactive_commands/autocommit.md) is set to **off** (**false**). </br> Queries on column tables are supported when isolation level is set to `NONE`. TIBCO ComputeDB internally sets autocommit to `true` in this case.

    Queries on row tables are supported when **autocommit** is set to **false** and isolation level is set to other `READ_COMMITTED` or `REPEATABLE_READ`.

## Examples

!!! Note
	Before you try these examples, ensure that you have [started the TIBCO ComputeDB cluster](start_snappy_cluster.md).

The following examples provide JDBC example code snippets that explain how to use transactions isolation levels.


### **Example 1**

For row tables, **autocommit** can be set to **false** or **true**

```pre
import java.sql.{Connection, Statement}

...
...

val url: String = "jdbc:snappydata://1527/"
val conn1 = DriverManager.getConnection(url)
val stmt1 = conn1.createStatement()

// create a row table
stmt1.execute("CREATE TABLE APP.PARTSUPP ( " +
        "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
        "PS_SUPPKEY     INTEGER NOT NULL," +
        "PS_AVAILQTY    INTEGER NOT NULL," +
        "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
        "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY')")

// set the tx isolation level
conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
// set autocommit to false
conn1.setAutoCommit(false)

val preparedStmt1 = conn1.prepareStatement("INSERT INTO APP.PARTSUPP VALUES(?, ?, ?, ?)")
for (x <- 1 to 10) {
  preparedStmt1.setInt(1, x*100)
  preparedStmt1.setInt(2, x)
  preparedStmt1.setInt(3, x*1000)
  preparedStmt1.setBigDecimal(4, java.math.BigDecimal.valueOf(100.2))
  preparedStmt1.executeUpdate()
}

// commit the transaction
conn1.commit()

val rs1 = stmt1.executeQuery("SELECT * FROM APP.PARTSUPP")
while (rs1.next()) {
  println(rs1.getInt(1) + "," + rs1.getInt(2) + "," + rs1.getInt(3))
}
rs1.close()
stmt1.close()

conn1.close()
```

### **Example 2**

For column tables, **autocommit** must be set to **true**, otherwise, an error is reported when the query is executed.

```pre
val conn2 = DriverManager.getConnection(url)
val stmt2 = conn2.createStatement()

// create a column table
stmt2.execute("CREATE TABLE CUSTOMER ( " +
    "C_CUSTKEY     INTEGER," +
    "C_NAME        VARCHAR(25)," +
    "C_ADDRESS     VARCHAR(40)," +
    "C_NATIONKEY   INTEGER," +
    "C_PHONE       VARCHAR(15)," +
    "C_ACCTBAL     DECIMAL(15,2)," +
    "C_MKTSEGMENT  VARCHAR(10)," +
    "C_COMMENT     VARCHAR(117))" +
    "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY')")

// set the tx isolation level
conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
// set autocommit to true otherwise opeartions on column table will error out
conn2.setAutoCommit(true)
stmt2.execute("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")
stmt2.execute("INSERT INTO CUSTOMER VALUES(30000, 'Customer30000', " +
        "'San Hose, CA', 1, '555-201-562', 4500, 'MKTSEGMENT', '')")
val rs2 = stmt2.executeQuery("SELECT * FROM APP.CUSTOMER")
while (rs2.next()) {
  println(rs2.getInt(1) + "," + rs2.getString(2))
}
rs2.close()
```

#### Unsupported operations when **autocommit** is set to false for column tables

```pre
// if autocommit is set to false, queries throw an error if column tables are involved
conn2.setAutoCommit(false)
// invalid query
stmt2.execute("SELECT * FROM APP.CUSTOMER")
// the above statement throws an error as given below
EXCEPTION: java.sql.SQLException: (SQLState=XJ218 Severity=20000) (Server=localhost/127.0.0.1[25299] Thread=pool-14-thread-3) Operations on column tables are not supported when query routing is disabled or autocommit is false
```

**More information**

- [Overview of TIBCO ComputeDB Distributed Transactions](../consistency/transactions_about.md)

- [Best Practices for TIBCO ComputeDB Distributed Transactions](../best_practices/transactions_best_practices.md)
