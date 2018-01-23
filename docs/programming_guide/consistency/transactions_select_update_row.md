# Transactions with SELECT FOR UPDATE

### we don't support Select for update. Remove this entire section after confirming from Suranjan/sumedh

!!!Hint:
	Distributed transaction is supported only for row tables.

The `SELECT FOR UPDATE` statement and other statements that implicitly place locks are not supported outside of a transaction (default isolation level).

A SELECT FOR UPDATE begins by obtaining a read lock, which allows other transactions to possibly obtain read locks on the same data. A transaction's read lock is immediately upgraded to an exclusive write lock after a row is qualified for the SELECT FOR UPDATE statement. At this point, any other transactions that obtained a read lock on the data receive a conflict exception and can roll back and release their locks.

The transaction that has the exclusive lock can successfully commit only after all other read locks on the table have been released. In some cases, it is possible for one transaction to obtain an exclusive lock for data on one SnappyData member, while another transaction obtains an exclusive lock on a different member. In this case, both transactions will fail during the commit.


