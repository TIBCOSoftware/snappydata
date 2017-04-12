# gemfire.LOCK_MAX_TIMEOUT

## Description

This is the maximum time to wait for a transaction that is committing a row that the current transaction is trying to read or write. Do not change this property unless transactions involve a large number of writes and potentially can take a very long time during commit to write to datastores. The default is 5 minutes.

If you change this property, set it to the same value on every data store member in your distributed system.

This property configures conflict detection for READ\_COMMITTED and REPEATABLE\_READ transactions. See <a href="../../developers_guide/topics/queries/transactions-isolation-levels.md#concept_830FC26DAE844CAB933FF3CEEDCB2535" class="xref" title="RowStore supports several transaction isolation levels. It does not support the SERIALIZABLE isolation level, nested transactions, or savepoints.">Supported Transaction Isolation Levels</a>.

## Default Value

5

## Property Type

system

## Prefix

n/a
