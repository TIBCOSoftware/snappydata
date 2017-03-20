# java.sql.BatchUpdateException Class

<a id="java-sql-connection__section_DACA1A9897F84B6EA87772D5FEEAC856"></a>
This class is an extension of SQLException that is thrown if a failure occurs during execution of a batch statement. Such a failure can occur with the batch operation of the JDBC Statement, PreparedStatement or CallableStatement classes. Exceptions caused by other batch elements are maintained in the getNextException() chain, as they are for SQLExceptions.

This class adds one new method to the SQLException class named getUpdateCounts(). This method returns an array of update counts of the statements that executed successfully before the exception occurred.


