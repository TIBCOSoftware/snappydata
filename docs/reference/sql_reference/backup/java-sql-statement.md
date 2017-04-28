# java.sql.Statement Class

SnappyData does not implement the `setEscapeProcessing` method of `java.sql.Statement`. In addition, the cancel method raises a "Feature not supported" exception.

## ResultSet Objects

An error that occurs when a SELECT statement is first executed prevents a `ResultSet` object from being opened on it. The same error does not close the `ResultSet` if it occurs after the `ResultSet` is opened. For example, a divide-by-zero error that occurs while the `executeQuery` method is called on a `java.sql.Statement` or `java.sql.PreparedStatement` throws an exception and returns no result set at all, while if the same error occurs while the `next` method is called on a `ResultSet` object, it does not close the result set.

Errors can occur when a `ResultSet` is first being created if the system partially executes the query before the first row is fetched. This errors can occur on any query that uses more than one table and on queries that use aggregates, GROUP BY, ORDER BY, DISTINCT, INTERSECT, EXCEPT, or UNION. (See <a href="../language_ref/ref-sql-limitations.html#concept_05E66BCA75DD4940994906F0BF31AE17" class="xref" title="SnappyData has limitations and restrictions for SQL statements, clauses, and expressions.">Product Limitations</a>.) Closing a `Statement` causes all open `ResultSet` objects on that statement to be closed as well.The cursor name for the cursor of a `ResultSet` can be set before the statement is executed. However, once it is executed, the cursor name cannot be altered.
