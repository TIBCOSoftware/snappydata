# java.sql.Connection Interface

<a id="java-sql-connection__section_DACA1A9897F84B6EA87772D5FEEAC856"></a>
A RowStore Connection object is not garbage-collected until all other JDBC objects created from that connection are explicitly closed or are themselves garbage-collected. Once the connection is closed, no further JDBC requests can be made against objects created from the connection. Do not explicitly close the Connection object until you no longer need it for executing statements.

A session-severity or higher exception causes the connection to close and all other JDBC objects against it to be closed.

<a id="java-sql-connection__section_9150048F0190468991B70B87ADC47DA7"></a>

##java.sql.Connection.setTransactionIsolation Method


Only java.sql.Connection.TRANSACTION\_NONE, and java.sql.Connection.TRANSACTION\_READ\_COMMITTED transaction isolation levels are available in RowStore.

TRANSACTION\_NONE is the default isolation level.

Changing the current isolation for the connection with *setTransactionIsolation* commits the current transaction and begins a new transaction.


!!! Note
		RowStore provides atomicity and thread safety for row-level operations even in TRANSACTION\_NONE isolation level.

For more details about transaction isolation, see <a href="../../developers_guide/topics/queries/transactions.html#transactions" class="xref" title="A transaction is a set of one or more SQL statements that make up a logical unit of work that you can commit or roll back, and that will be recovered in the event of a system failure. RowStore&#39;s unique design for distributed transactions allows for linear scaling without compromising atomicity, consistency, isolation, and durability (ACID) properties.">Transactions and Concurrency</a>.

<a id="java-sql-connection__section_C205B25369BE4992BC07C59D285AF972"></a>

##Connection Functionality Not Supported

*java.sql.Connection.setReadOnly* and *isReadOnly* methodsRead-only connections are not supported in RowStore. Calling *setReadOnly* with a value of true throws a "Feature not supported" exception, and *isReadOnly* always returns false.

Connections to XA DataSources (javax.sql.XADataSource) are not supported in this release of RowStore.

RowStore does not use catalog names. In addition, the following optional methods raise "Feature not supported" exceptions:

-   *createArrayOf( java.lang.String, java.lang.Object\[\] )*

-   *createNClob( )*

-   *createSQLXML( )*

-   *createStruct( java.lang.String, java.lang.Object\[\] )*

-   *getTypeMap( )*

-   *setTypeMap( java.util.Map )*


