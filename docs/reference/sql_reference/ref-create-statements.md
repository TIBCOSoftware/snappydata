# CREATE Statements

Use Create statements to create functions, indexes, procedures, schemas, synonyms, tables, triggers, and views.

-   **[CREATE ALIAS](../../reference/language_ref/ref-create-alias.html)**
    Creates an alias for a user-defined procedure result processor.
-   **[CREATE ASYNCEVENTLISTENER](../../reference/language_ref/ref-create-async-event-listener.html)**
    Installs an AsyncEventListener implementation to SnappyData peers in a specified server group.
-   **[CREATE DISKSTORE](../../reference/language_ref/ref-create-diskstore.html)**
    Disk stores provide disk storage for tables and queues that need to overflow or persist (for instance when using an asynchronous write-behind listener).
-   **[CREATE FUNCTION](../../reference/language_ref/ref-create-function.html)**
    Create Java functions, which you can then use in an expression.
-   **[CREATE GATEWAYRECEIVER](../../reference/language_ref/ref-create-gateway-receiver.html)**
    Creates a gateway receiver to replicate data from a remote SnappyData cluster.
-   **[CREATE GATEWAYSENDER](../../reference/language_ref/ref-create-gateway-sender.html)**
    Creates a gateway sender to replicate data to a remote SnappyData cluster.
-   **[CREATE GLOBAL HASH INDEX](../../reference/language_ref/ref-create-global-hash-index.html)**
    Creates an index that contains unique values across all of the members that host a partitioned table's data.
-   **[CREATE HDFSSTORE](../../reference/language_ref/ref-create-hdfs-store.html)**
    Creates a connection to a Hadoop name node in order to persist one or more tables to HDFS. Each connection defines the HDFS NameNode and directory to use for persisting data, as well as SnappyData-specific options to configure the queue used to persist table events, enable persistence for the connection, compact the HDFS operational logs, and so forth.
-   **[CREATE INDEX](../../reference/language_ref/ref-create-index.html)**
    Creates an index on one or more columns of a table.
-   **[CREATE PROCEDURE](../../reference/language_ref/ref-create-procedure.html)**
    Creates a Java stored procedure that can be invoked using the CALL statement.
-   **[CREATE SCHEMA](../../reference/language_ref/ref-create-schema.html)**
    Creates a schema with the given name which provides a mechanism to logically group objects.
-   **[CREATE SYNONYM](../../reference/language_ref/ref-create-synonym.html)**
    Provide an alternate name for a table or view.
-   **[CREATE TABLE](../../reference/language_ref/ref-create-table.html)**
    Creates a new table using SnappyData features.
-   **[CREATE TRIGGER](../../reference/language_ref/ref-create-trigger.html)**
    A trigger defines a set of actions that are executed when a delete, insert, or update operation is performed on a table. For example, if you define a trigger for a delete on a particular table, the trigger's action occurs whenever someone deletes a row or rows from the table.
-   **[CREATE TYPE](../../reference/language_ref/rrefsqljcreatetype.html)**
-   **[CREATE VIEW](../../reference/language_ref/ref-create-view.html)**
    Views are virtual tables formed by a query.


