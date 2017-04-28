# java.sql.Connection Interface

<a id="java-sql-connection-interface"></a>
A SnappyData Connection object is not garbage-collected until all other JDBC objects created from that connection are explicitly closed or are themselves garbage-collected. Once the connection is closed, no further JDBC requests can be made against objects created from the connection. Do not explicitly close the Connection object until you no longer need it for executing statements.

A session-severity or higher exception causes the connection to close and all other JDBC objects against it to be closed.

<a id="java-sql-connection"></a>

##java.sql.Connection.setTransactionIsolation Method


Only java.sql.Connection.TRANSACTION_NONE, and java.sql.Connection.TRANSACTION_READ_COMMITTED transaction isolation levels are available in SnappyData.

TRANSACTION_NONE is the default isolation level.

Changing the current isolation for the connection with *setTransactionIsolation* commits the current transaction and begins a new transaction.


!!! Note
		SnappyData provides atomicity and thread safety for row-level operations even in TRANSACTION_NONE isolation level.

For more details about transaction isolation, see <mark> TO BE CONFIRMED RowStore link Transactions and Concurrency </mark>. 

<a id="connection-functionality"></a>

##Connection Functionality Not Supported

*java.sql.Connection.setReadOnly* and *isReadOnly* methodsRead-only connections are not supported in SnappyData. Calling *setReadOnly* with a value of true throws a "Feature not supported" exception, and *isReadOnly* always returns false.

Connections to XA DataSources (javax.sql.XADataSource) are not supported in this release of SnappyData.

SnappyData does not use catalog names. In addition, the following optional methods raise "Feature not supported" exceptions:

-   *createArrayOf( java.lang.String, java.lang.Object\[\] )*

-   *createNClob( )*

-   *createSQLXML( )*

-   *createStruct( java.lang.String, java.lang.Object\[\] )*

-   *getTypeMap( )*

-   *setTypeMap( java.util.Map )*


