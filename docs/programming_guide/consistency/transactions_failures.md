# Rollback Behavior and Member Failures

!!!Hint:
	Distributed transaction is supported only for row tables.

Within the scope of a transaction, SnappyData automatically initiates a rollback if it encounters a constraint violation.

Any errors that occur while parsing queries or while binding parameters in a SQL statement *do not* cause a rollback. For example, a syntax error that occurs while executing a SQL statement does not cause previous statements in the transaction to rollback. However, a column constraint violation would cause all previous SQL operations in the transaction to roll back.

## Handling Member Failures

The following steps describe specific events that can occur depending on which member fails and when the failure occurs during a transaction:

1.  If the transaction coordinator member fails before a commit is fired, then each of the cohort members aborts the ongoing transaction.
2.  If a participating member fails before commit is fired, then it is simply ignored. If the copies/replicas go to zero for certain keys, then any subsequent update operations on those keys throws an exception as in the case of non-transactional updates. If a commit is fired in this state, then the whole transaction is aborted.
3.  If the transaction coordinator fails before completing the commit process (with or without sending the commit message to all cohorts), the surviving cohorts determine the outcome of the transaction.

    If all of the cohorts are in the PREPARED state and successfully apply changes to the cache without any unique constraint violations, the transaction is committed on all cohorts. Otherwise, if any member reports failure or the last copy the associated rows goes down during the PREPARED state, the transaction is rolled back on all cohorts.

4.  If a participating member fails before acknowledging to the client, then the transaction continues on other members without any interruption. However, if that member contains the last copy of a table or bucket, then the transaction is rolled back.
5.  The transaction coordinator might also fail while executing a rollback operation. In this case, a thin client would see such a failure as a SQLState error. If the client was performing a SELECT statement in a transaction, the member failure would result in SQLState error X0Z01::

    ``` pre
    ERROR X0Z01: Node 'node-name' went down or data no longer available while iterating the results (method 'rollback()'). Please retry the operation. 
    ```

    Clients that were performing a DML statement in the context of a transaction would fail with one of the SQLState errors: X0Z05, X0Z16, 40XD2, or 40XD0.

    !!! Note:
    	Outside the scope of a transaction, a DML statement would not see an exception due to a member failure. Instead, the statement would be automatically retried on another SnappyData member. However, SELECT statements would receive the X0Z01 statement even outside of a transaction.</p>

Should this type of failure occur, the remaining members of the SnappyData distributed system clean up the open transactions for the failed node, and no additional steps are needed to recover from the failure. A peer client connection would not see this exception because the peer client itself acts as the transaction coordinator.

!!! Note:
	In this release of SnappyData, a transaction fails if any of the cohorts depart abnormally. 

<a id="rollback_scenarios"></a>
## Other Rollback Scenarios

SnappyData may cancel an executing statement due to low memory, a timeout, or a manual request to cancel the statement 
<!-- see (<a href="../../../manage_guide/Topics/cancelling-queries.html#concept_wjv_mq1_rn" class="xref" title="When managing a SnappyData deployment, it may become necessary to cancel statements that are taking too long to complete, or that are causing bottlenecks in your system. SnappyData supports canceling queries using either a system procedure or the JDBC Statement.cancel() API.">Cancelling Long-Running Statements</a>)-->.

If a statement that is being executed within the context of a transaction is canceled due to low memory or a manual cancellation request, then SnappyData rolls back the associated transaction. Note that SnappyData does not roll back a transaction if a statement is canceled due to a timeout.


