# Known Issues 
The following key issues have been registered as defects in the TIBCO ComputeDB defect tracking system:

<table align="left">
<colgroup>
<col width="25%" />
<col width="25%" />
<col width="25%" />
<col width="25%" />
</colgroup>
<thead>
<tr class="header">
<th>BUG ID</th>
<th>Title</th>
<th>Description</th>
<th>Workaround</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-1375">SNAP-1375</a></td>
<td>JVM crash reported</td>
<td>This was reported on: <br> - RHEL kernel version: 3.10.0-327.13.1.el7.x86_64 <br> - Java version: 1.8.0_121</td>
<td>To resolve this, use: </br> - RHEL kernel version: 3.10.0-693.2.2.el7.x86_64 </br> - Java version: 1.8.0_144</td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-1422">SNAP-1422</a></td>
<td>Catalog in smart connector inconsistent with servers</td>
<td>Catalog in smart connector inconsistent with servers|When a table is queried from spark-shell (or from an application that uses smart connector mode) the table metadata is cached on the smart connector side. </br>If this table is dropped from TIBCO ComputeDB embedded cluster (by using Snappy Shell, or JDBC application, or a Snappy job), the metadata on the smart connector side stays cached even though catalog has changed (table is dropped). </br>In such cases, the user may see unexpected errors like "org.apache.spark.sql.AnalysisException: Table `SNAPPYTABLE` already exists"  in the smart connector app side for example for `DataFrameWriter.saveAsTable()` API if the same table name that was dropped is used in `saveAsTable()`</td>
<td> 
1. User may either create a new Snappy Session in such scenarios </br>OR </br> 
2. Invalidate the cache on the Smart Connector mode, for example by calling </br>  `snappy.sessionCatalog.invalidateAll()`</td>
</tr>
<tr class="even">
<td><a href="https://jira.snappydata.io/browse/SNAP-1634">SNAP-1634</a></td>
<td>Creating a temporary table with the same name as an existing table in any schema should not be allowed</td>
<td>When creating a temporary table, the TIBCO ComputeDB catalog is not referred, which means, a temporary table with the same name as that of an existing TIBCO ComputeDB table can be created. Two tables with the same name lead to ambiguity during query execution and can either cause the query to fail or return wrong results. </br></td>
<td> Ensure that you create temporary tables with a unique name. </td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-1753">SNAP-1753</a></td>
<td>TPCH Q19 execution performance degraded in 0.9</td>
<td>A disjunctive query (that is, query with two or more predicates joined by an OR clause) with common filter predicates may report performance issues.</td>
<td>To resolve this, the query should be rewritten in the following manner to achieve better performance:
<pre class="pre"><code>  
select
        sum(l_extendedprice) 
    from
        LINEITEM,
        PART
    where
        (
       p_partkey = l_partkey
       and p_size between 1 and 5
 and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
       p_partkey = l_partkey
       and p_brand = 'Brand#?'
       and l_shipinstruct = 'DELIVER IN PERSON'
        )
</code></pre>
<pre class="pre"><code>
select
        sum(l_extendedprice)
    from
        LINEITEM,
        PART
    where
        ( p_partkey = l_partkey and l_shipinstruct = 'DELIVER IN PERSON') and 
        ( p_size between 1 and 5 or  p_brand = 'Brand#3')
</code></pre>
</td>
</tr>
<tr class="even">
<td><a href="https://jira.snappydata.io/browse/SNAP-1911">SNAP-1911</a></td>
<td>JVM crash reported</td>
<td>This was reported on: <br> -  RHEL kernel version: 3.10.0-327.13.1.el7.x86_64<br> - Java version: 1.8.0_131</td>
<td>To resolve this, use: </br> - RHEL kernel version: 3.10.0-693.2.2.el7.x86_64</br> - Java version: 1.8.0_144</td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-1999">SNAP-1999</a></td>
<td>JVM crash reported</td>
<td>This was reported on: <br> - RHEL kernel version: 3.10.0-327.13.1.el7.x86_64 <br> - Java version: 1.8.0_131</td>
<td>To resolve this, use: </br> - RHEL kernel version: 3.10.0-693.2.2.el7.x86_64 </br> - Java version: 1.8.0_144</td>
</tr>
<tr class="even">
<td><a href="https://jira.snappydata.io/browse/SNAP-2017">SNAP-2017</a></td>
<td>JVM crash reported</td>
<td>This was reported on: <br> - RHEL kernel version: 3.10.0-514.10.2.el7.x86_64 <br> - Java version: 1.8.0_144</td>
<td>To resolve this, use: </br> -  RHEL kernel version: 3.10.0-693.2.2.el7.x86_64 </br> - Java version: 1.8.0_144</td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-2436">SNAP-2436</a></td>
<td>Data mismatch in queries running on servers coming up after a failure</td>
<td>Data mismatch is observed in queries which are running when some servers are coming up after a failure. Also, the tables on which the queries are running must have set their redundancy to one or more for the issue to be observed. </td>
<td>This issue happens due to Spark retry mechanism with TIBCO ComputeDB tables. To avoid this issue, you can stop all the queries when one or more servers are coming up. If that is not feasible, you should configure the lead node with `spark.task.maxFailures = 0`; </td>
</tr>
<tr class="even">
<td><a href="https://jira.snappydata.io/browse/SNAP-2381">SNAP-2381</a></td>
<td>Data inconsistency due to concurrent putInto/update operations</td>
<td>Concurrent putInto/update operations and inserts in column tables with overlapping keys may cause data inconsistency.  </td>
<td>This problem is not seen when all the concurrent operations deal with different sets of rows. You can either ensure serialized mutable operations on column tables or these should be working on a distinct set of key columns.</td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-2457">SNAP-2457</a></td>
<td>Inconsistent results during further transformation when using snappySession.sql() from jobs, Zeppelin etc. </td>
<td>When using snappySession.sql() from jobs, Zeppelin etc, if a further transformation is applied on the DataFrame, it may give incorrect results due to plan caching.  </td>
<td>If you are using SnappyJobs and using snappySession.sql("sql string") you must ensure that further transformation is not done. For example:

<pre class="pre"><code>  
val df1 = snappySession.sql("sql string")
val df2 = df1.repartition(12) // modifying df1
df2.collect()
</code></pre>

The above operation will give inconsistent results, if you are using df2 further in your code.
To avoid this problem, you can use snappySession.sqlUncached("sql string"). For example:

<pre class="pre"><code> 
val df1 = snappySession.sqlUncached("sql string")
val df2 = df1.repartition(12) // modifying df1
df2.collect()
</code></pre></td>
</tr>

<!--
<tr class="even">
<td><a href="https://jira.snappydata.io/browse/SNAP-1153">SNAP-1153</a></td>
<td></td>
<td></td>
<td></td>
</tr>
-->
</table>

<!-- 
Format for new rows
<tr class="odd">
<td></td>
<td></td>
<td></td>
<td></td>
</tr>
<tr class="even">
<td></td>
<td></td>
<td></td>
<td></td>
</tr>
-->
