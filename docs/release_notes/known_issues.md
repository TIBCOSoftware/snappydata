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
<td>SNAP-1422</td>
<td>Catalog in smart connector inconsistent with servers</td>
<td>Catalog in smart connector inconsistent with servers|When a table is queried from spark-shell (or from an application that uses smart connector mode) the table metadata is cached on the smart connector side. </br>If this table is dropped from SnappyData embedded cluster (by using snappy-shell, or JDBC application, or a Snappy job), the metadata on the smart connector side stays cached even though catalog has changed (table is dropped). </br>In such cases, the user may see unexpected errors like "org.apache.spark.sql.AnalysisException: Table `SNAPPYTABLE` already exists"  in the smart connector app side for example for `DataFrameWriter.saveAsTable()` API if the same table name that was dropped is used in `saveAsTable()`</td>
<td> 
1. User may either create a new SnappySession in such scenarios </br>OR </br> 
2. Invalidate the cache on the Smart Connector mode, for example by calling </br>  `snappy.sessionCatalog.invalidateAll()`</td>
</tr>
</table>
<!--
<tr class="even">
<td>SNAP-1753</td>
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
-->

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
