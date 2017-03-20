#Statements
Learn about the supported statements provided in this release of RowStore.

Each reference page provides the statement syntax, describes custom extensions, and shows examples of using the statement.

-   **[ALTER TABLE](../../reference/language_ref/ref-alter-table.html)**
    Use the ALTER TABLE statement to add columns and constraints to an existing table, remove them from a table, or modify other table features such as AsyncEventListener implementations and gateway sender configurations.
-   **[CALL](../../reference/language_ref/ref-call-procedure.html)**
    RowStore extends the CALL statement to enable execution of Data-Aware Procedures (DAP). These procedures can be routed to RowStore members that host the required data.
-   **[CREATE Statements](../../reference/language_ref/ref-create-statements.html)**
    Use Create statements to create functions, indexes, procedures, schemas, synonyms, tables, triggers, and views.
-   **[DECLARE GLOBAL TEMPORARY TABLE](../../reference/language_ref/ref-declare-global-temporary-table.html)**
    Defines a temporary table for the current connection uniquely identified within the local member.
-   **[DELETE](../../reference/language_ref/ref-delete.html)**
    Delete rows from a table.
-   **[EXPLAIN](../../reference/language_ref/ref-explain.html)**
    Capture or display the query execution plan for a statement.
-   **[DROP statements](../../reference/language_ref/ref-drop.html)**
    Drop statements are used to drop functions, indexes, procedures, schemas, tables, triggers, and views.
-   **[GRANT](../../reference/language_ref/ref-grant.html)**
    Enable permissions for a specific user or all users.
-   **[INSERT](../../reference/language_ref/ref-insert.html)**
    An INSERT statement creates a row or rows and stores them in the named table. The number of values assigned in an INSERT statement must be the same as the number of specified or implied columns.
-   **[PUT INTO](../../reference/language_ref/put-into.html)**
    Creates or replaces rows in a table without first checking existing primary key values. If existing rows with the same primary key are present in the table, they are overwritten with the new values. This syntax can be used to speed insert operations when importing data. It is also helpful when working with HDFS-persistent tables where checking primary key values requires data to be retrieved from HDFS.
-   **[REVOKE](../../reference/language_ref/ref-revoke.html)**
    Revoke privileges to a table or to a routine.
-   **[SELECT](../../reference/language_ref/ref-select.html)**
    A query with an optional ORDER BY CLAUSE and an optional FOR UPDATE clause.
-   **[SET ISOLATION](../../reference/store_commands/set_isolation.html)**
    Change the transaction isolation level for the connection.
-   **[SET SCHEMA](../../reference/language_ref/ref-set-schema.html)**
    Set or change the default schema for a connection's session.
-   **[TRUNCATE TABLE](../../reference/language_ref/ref-truncate-table.html)**
    Remove all content from a table and return it to its initial, empty state. TRUNCATE TABLE clears all in-memory data for the specified table as well as any data that was persisted to RowStore disk stores. For HDFS read-write tables, TRUNCATE TABLE also marks the table's HDFS persistence files for expiiry. For HDFS write-only tables, TRUNCATE TABLE leaves all table data that is stored in HDFS log files available for later processing using MapReduce or HAWQ.
-   **[UPDATE](../../reference/language_ref/ref-update.html)**
    Update the value of one or more columns.


