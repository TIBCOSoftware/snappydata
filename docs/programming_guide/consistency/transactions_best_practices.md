# Best Practices for Using Transactions

-   For high performance, mimimize the duration of transactions to avoid conflicts with other concurrent transactions. If atomicity for only single row updates is required, then completely avoid using transactions because SnappyData provides atomicity and isolation for single rows without transactions.

-   When using transactions, keep the number of rows involved in the transaction as low as possible. SnappyData acquires locks eagerly, and long-lasting transactions increase the probability of conflicts and transaction failures. Avoid transactions for large batch update statements or statements that effect a lot of rows. 

-   Unlike in traditional databases, SnappyData transactions can fail with a conflict exception on writes instead of on commit. This choice makes sense given that the outcome of the transaction has been determined to fail.

-   To the extent possible, model your database so that most transactions operate on colocated data. When all transactional data is on a single member, then stricter isolation guarantees are provided.

## This below section doesn't make sense ... sumedh to clarify

-   If your application is multi-threaded, consider setting the sync-commits </a> connection property to "true." By default SnappyData performs second-phase commit actions in the background, but ensures that the connection that issued the transaction only sees committed state. However, other threads or connections may see different results until the second-phase commit actions complete. Setting `sync-commits=true` ensures that the current thin client or peer client connection waits until all second-phase commit actions complete.


