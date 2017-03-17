# java.sql.SQLException Class

<a id="java-sql-sqlexception__section_8DF3079FF6824831A6FE6D41C807D0AA"></a>
SQLExceptions that are thrown by RowStore do not always match the Exception that a Derby database would throw. The `getSQLState()` method on `SQLException` may also return a different value in some cases.


