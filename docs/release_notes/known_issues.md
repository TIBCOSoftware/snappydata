# Known Issues 
The following key issues have been registered as bugs in the SnappyData bug tracking system:

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
<td><a href="https://jira.snappydata.io/browse/SNAP-1422">SNAP-1422</a></td>
<td>Catalog in smart connector inconsistent with servers</td>
<td>Catalog in smart connector inconsistent with servers|When a table is queried from spark-shell (or from an application that uses smart connector mode) the table metadata is cached on the smart connector side. </br>If this table is dropped from SnappyData embedded cluster (by using snappy-shell, or JDBC application, or a Snappy job), the metadata on the smart connector side stays cached even though catalog has changed (table is dropped). </br>In such cases, the user may see unexpected errors like "org.apache.spark.sql.AnalysisException: Table `SNAPPYTABLE` already exists"  in the smart connector app side for example for `DataFrameWriter.saveAsTable()` API if the same table name that was dropped is used in `saveAsTable()`</td>
<td> 
1. User may either create a new SnappySession in such scenarios </br>OR </br> 
2. Invalidate the cache on the Smart Connector mode, for example by calling </br>  `snappy.sessionCatalog.invalidateAll()`</td>
</tr>
<tr class="odd">
<td><a href="https://jira.snappydata.io/browse/SNAP-1634">SNAP-1634</a></td>
<td>Creating a temporary table with the same name as an existing table in any schema should not be allowed</td>
<td>When creating a temporary table, the SnappyData catalog is not referred, which means, a temporary table with the same name as that of an existing SnappyData table can be created. Two tables with the same name lead to ambiguity during query execution and can either cause the query to fail or return wrong results. </br></td>
<td> Ensure that you create temporary tables with a unique name. </td>
</tr>
<tr class="even">
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