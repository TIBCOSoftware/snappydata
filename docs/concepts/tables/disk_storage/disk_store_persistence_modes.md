# Disk Store Persistence Attributes

SnappyData persists data on disk stores in synchronous or asynchronous mode.

You configure the persistence mode for a table in the [CREATE TABLE](../../../reference/sql_reference/create-table.md) statement, while attributes to control asynchronous persistence are configured in the [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md) statement.

In synchronous mode, SnappyData writes each DML operation to the OS buffer as part of the statement execution. This mode provides greater reliability than asynchronous mode, but with lower performance.

In asynchronous mode, SnappyData batches DML statements before flushing them to the OS buffers. This is faster than synchronous mode, but batch operations may be lost if a failure occurs. (You can use redundancy to ensure that updates are successfully logged on another machine.) In asynchronous mode, you can control the frequency of flushing batches by setting the following attributes when you create a named disk store:

-   **QUEUESIZE** sets the number of affected rows that can be asynchronously queued. After this number of pending rows are queued, operations begin blocking until some of the modified, created, or deleted rows are flushed to disk.

-   **TIMEINTERVAL** sets the number of milliseconds that can elapse before queued data is flushed to disk.

See [CREATE DISKSTORE](../../../reference/sql_reference/create-diskstore.md)

!!! Note:
	* Always start all SnappyData servers and peer processes that host data on disk in parallel. A SnappyData process with persistent data may wait for other processes to startup first to guarantee consistency.

	* Always use the shutdown-all command to gracefully shut down a cluster. This allows each member to reach a consistent replication state and record that state with other replicas in the cluster. When you restart peers after a graceful shutdown, each member can recover in parallel without waiting for others to provide consistent replication data.

