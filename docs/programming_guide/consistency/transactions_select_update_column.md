# Snapshot Isolation with SELECT FOR UPDATE

The `SELECT FOR UPDATE` statement and other statements that implicitly place locks are not supported for column tables, and Snapshot isolation is applied by default for updates. In case of multiple concurrent updates the last update is applied.

# Rollback Behavior and Member Failures

In column tables, roll back is performed in case of low memory. If the operation fails due to low memory, automatic roll back is initiated.

# Best Practices

To the extent possible, model your database so that most transactions operate on colocated data. When all transactional data is on a single member, then stricter isolation guarantees are provided. In case of failure, the rollback is complete and not partial.

# Snapshot Limitations

The following limitations have been reported:
- For column tables Snapshot isolation is enabled by default, but the full range of fault tolerance is not yet implemented. It is assumed that at least one copy of a partition is always available (redundant members are available) in the event of member failures.
- Write-write conflict is not detected. The last write option is applied.
- Multi-statement is not supported.