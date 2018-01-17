# Best Practices for Using Transactions

For optimum results, take note of best practices for working with SnappyData transactions.

-   For high performance, mimimize the duration of transactions to avoid conflicts with other concurrent transactions. If atomicity for only single row updates is required, then completely avoid using transactions because SnappyData provides atomicity and isolation for single rows without transactions.

-   When using transactions, keep the transaction duration and the number of rows involved in the transaction as low as possible. SnappyData acquires locks eagerly, and long-lasting transactions increase the probability of conflicts and transaction failures.

-   Unlike in traditional databases, SnappyData transactions can fail with a conflict exception on updates instead of on commit. This choice makes sense given that the outcome of the transaction has been determined to fail.

    Conflicts may be exacerbated by triggers, because triggers are executed on different members and may attempt to update the same data rows concurrently. Batched updates can also compound these failures because SnappyData can lazily perform conflict detection at any point before the the transaction is committed (instead of at the time of the update, as with non-batched transactions). When such a conflict occurs, it is common for one or all concurrent transactions to fail and roll back.

-   To the extent possible, model your database so that most transactions operate on colocated data. When all transactional data is on a single member, then stricter isolation guarantees are provided.

-   If your application spawns multiple threads or connections to work on committed data, consider setting the sync-commits </a> connection property to "true." By default SnappyData performs second-phase commit actions in the background, but ensures that the connection that issued the transaction only sees completed results. However, other threads or connections may see different results until the second-phase commit actions complete. Setting `sync-commits=true` ensures that the current thin client or peer client connection waits until all second-phase commit actions complete.


