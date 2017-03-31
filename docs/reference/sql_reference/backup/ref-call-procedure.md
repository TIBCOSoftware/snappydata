# CALL

SnappyData extends the CALL statement to enable execution of Data-Aware Procedures (DAP). These procedures can be routed to SnappyData members that host the required data.

##Syntax

``` pre
CALL procedure_name
 ( [ expression [, expression ]* ] )
 [ WITH RESULT PROCESSOR processor_class ]
 [ { ON TABLE table_name [ WHERE whereClause ] }
   |
   { ON { ALL | SERVER GROUPS (server_group_name [, server_group_name]* ) } }
 ]
```

<a id="reference_47BF2460F6924D4F8CD81DA99BFAD783__section_7ED8E756C2304C88947F7954A54EA3B5"></a>
##Description

The CALL syntax enables you to specify the SnappyData members that execute the procedure as well as the result processor that you want to use to process the results from multiple members.

SnappyData has a default result processor that collates results from different members if you do not specify a custom result processor. The default processor performs unordered merges on the dynamic result sets from each server where the procedure is executed, and presents the same number of ResultSets to the JDBC client that was declared in the CREATE PROCEDURE statement.

<a href="../../developers_guide/topics/server-side/data-aware-procedures.html#data-aware-procedures" class="xref" title="A procedure is an application function call or subroutine that is managed in the database server. Because multiple SnappyData members operate together in a distributed system, procedure execution in SnappyData can also be parallelized to run on multiple members, concurrently. A procedure that executes concurrently on multiple SnappyData members is called a data-aware procedure.">Programming Data-Aware Procedures and Result Processors</a> provides more information about developing and configuring data-aware procedures and custom result processors.

<a id="reference_47BF2460F6924D4F8CD81DA99BFAD783__section_2844AFF97AB440D7BE175E11EB6A0D17"></a>

##WITH RESULT PROCESSOR


With this clause a custom result processor can be given which collates results and OUT parameters from multiple servers.

<a id="reference_47BF2460F6924D4F8CD81DA99BFAD783__section_EFD0580C0A0E4A9EB10914BF1BCD1C61"></a>

##ON and WHERE Clauses

The optional ON and WHERE clauses control the execution of DAPs on specific SnappyData members. If no ON clause is provided, the procedure is executed in only one server, the coordinator (data-independent). Otherwise, it is executed on only the servers that are hosting data for the specified table, optionally routed based on a WHERE clause. If an ON ALL or ON SERVER GROUPS clause is provided, then execution is routed to either all servers or the servers in the specified server groups.

##Example

This call executes the procedure "procedureName" only on those members that belong to server group "sg2:"

``` pre
CALL procedureName() ON SERVER GROUPS (sg2);
```

This call executes the procedure on members where values of ID are in the range 'ID &gt;= 20 and ID &lt; 40'. ID should be the partitioning column, otherwise SnappyData routes the procedure execution to all members that host data for the table:

``` pre
CALL procedureName() ON TABLE EMP.PARTITIONTESTTABLE WHERE ID >= 20 and ID < 40");
```

This call executes the procedure on all members (both accessors and data hosts):

``` pre
CALL procedureName() ON ALL;
```

The default behavior executes the procedure only on the query member:

``` pre
CALL procedureName();
```


