#SYS.CANCEL_STATEMENT

Attempt to cancel a SQL statement that is taking too long to complete or that is causing a bottleneck in your system.

##Syntax

!!!Note:
	See <a href="../../manage_guide/Topics/cancelling-queries.html#concept_wjv_mq1_rn" class="xref" title="When managing a SnappyData deployment, it may become necessary to cancel statements that are taking too long to complete, or that are causing bottlenecks in your system. SnappyData supports canceling queries using either a system procedure or the JDBC Statement.cancel() API.">Cancelling Long-Running Statements</a> for information about what statements are eligible for cancellation. A running statement that is cancelled manually throws a SQLState error XCL56.S: The statement has been cancelled due to a user request.</p>
``` pre
SYS.CANCEL_STATEMENT( 
    IN CURRENT_STATEMENT_UUID VARCHAR(1024) NOT NULL)
```

** CURRENT\_STATEMENT\_UUID  **
The UUID of the statement to cancel. You can obtain the CURRENT\_STATEMENT\_UUID by querying the SYS.SESSIONS table. This attribute cannot be null.

##Example

Query SYS.SESSIONS to obtain the CURRENT\_STATEMENT\_UUID of the statement that you want to cancel:

``` pre
snappy> select id, session_id, current_statement_uuid, current_statement, current_statement_status from sys.sessions;
ID                          |SESSION_ID |CURRENT_STATEMENT_UUID   |CURRENT_STATEMENT                                                                                                               |CURRENT_STATEMENT_STATUS
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
pnq-rdiyewar(7438)<v3>:27584|2          |12884901905-12884901918-1|SYSLH0001  select id, session_id, current_statement_uuid, current_statement, current_statement_status from sys.sessions          |EXECUTING STATEMENT     
pnq-rdiyewar(7194)<v2>:37098|2          |8589934609-8589934687-1  |SYSLH0001  select eqp_id, cntxt_id from CONTEXT_HISTORY where eqp_id||cast(cntxt_id as char(100)) in (select eqp_id||c&           |EXECUTING STATEMENT     
pnq-rdiyewar(6844):27404    |2          |17-19-1                  |SYSLH0001    call SYS.GET_ALLSERVERS_AND_PREFSERVER(?, ?, ?, ?)                                                                    |SENDING RESULTS         
pnq-rdiyewar(6844):27404    |3          |20-22-1                  |SYSLH0001    call SYS.GET_ALLSERVERS_AND_PREFSERVER(?, ?, ?, ?)                                                                    |SENDING RESULTS         

4 rows selected
```

Execute `SYS.CANCEL_STATEMENT()` with the UUID of the statement that you want to cancel:

``` pre
snappy> call sys.cancel_statement('8589934609-8589934687-1');
Statement executed.
```

!!! Note: 
	A successful return from `SYS.CANCEL_STATEMENT()` does not necessarily mean that SnappyData canceled the statement. It is possible that the statement completed successfully before the cancellation request was received, or that SnappyData was not be able to cancel the statement due to the current state of the operation. 


