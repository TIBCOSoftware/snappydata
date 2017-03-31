# CREATE INDEX

!!!Tip:
	This is only supported for row tables. Support for column tables will be provided in future releases.

Creates an index on one or more columns of a table.

##Syntax

``` pre
CREATE [ UNIQUE ] INDEX index_name
ON table-name (
column-name [ ASC | DESC ]
[ , column-name [ ASC | DESC ] ] * ) [ -- GEMFIREXD-PROPERTIES caseSensitive = { false | true} ]
```

##Description

The `CREATE INDEX` statement creates an index on one or more columns of a table. Indexes can speed up queries that use those columns for filtering data, or can also enforce a unique constraint on the indexed columns.

!!! Note:
	As a best practice, create indexes on all columns that are used to join tables. If an index is not available for a join query, SnappyData builds a hash table and uses the hash join strategy to improve query performance. However, joins that return a large number of rows can fail with an OVERFLOW\_TO\_DISK\_UNSUPPORTED exception if SnappyData does not have enough runtime memory to build the hash table. If you receive this error, check the joined columns and create indexes as necessary, or use an optimizer hint to change the selected join strategy. See <a href="../../manage_guide/Topics/optimizer-hints.html#concept_A93B4A631E7E42C39093021C2DAB1C74" class="xref" title="You can override the default behavior of the SnappyData query optimizer by including a -- GEMFIREXD-PROPERTIES clause and a property definition in a SQL statement. The clause and property definition both appear within the context of a SQL comment (beginning with two dashes &quot;--&quot;).">Overriding Optimizer Choices</a>.
The maximum number of columns for an index key in SnappyData is 16. An index name cannot exceed 128 characters. A column must not be named more than once in a single CREATE INDEX statement. Different indexes can name the same column, however.

SnappyData does not support creating an index on a column of datatype BLOB, CLOB, or LONG VARCHAR FOR BIT DATA. Indexes are supported for LONG VARCHAR columns.

SnappyData can use indexes to improve the performance of data manipulation statements. In addition, UNIQUE indexes provide a form of data integrity checking. However, the UNIQUE constraint only applies to the local member's data and not globally in the whole table. To enforce a unique index globally for a partitioned table, use the <a href="ref-create-global-hash-index.html#reference_B5512145CAC34AB08B245970E29E02FA" class="xref" title="Creates an index that contains unique values across all of the members that host a partitioned table&#39;s data.">CREATE GLOBAL HASH INDEX</a> statement.

Index names are unique within a schema. Some database systems allow different tables in a single schema to have indexes of the same name, but SnappyData does not. Both index and table are assumed to be in the same schema if a schema name is specified for one of the names, but not the other. If schema names are specified for both index and table, an exception will be thrown if the schema names are not the same. If no schema name is specified for either table or index, the current schema is used.

!!! Note:
	SnappyData implicitly creates an index on child table columns that define a foreign key reference. If you attempt to create an index on that same columns manually, you will receive a warning to indicate that the index is a duplicate of an existing index. See <a href="ref-create-table-clauses.html#topic_CF20DD46D0A0465E95B9682B6FEAFE43" class="xref" title="A CONSTRAINT clause is an optional part of a CREATE TABLE or ALTER TABLE statement that defines a rule to which table data must conform.">CONSTRAINT Clause</a>.
By default, SnappyData uses the ascending order of each column to create the index. Specifying ASC after the column name does not alter the default behavior. The DESC keyword after the column name causes SnappyData to use descending order for the column to create the index. Using the descending order for a column can help improve the performance of queries that require the results in mixed sort order or descending order and for queries that select the minimum or maximum value of an indexed column.

<a id="create-index__section_7228FA43433A46BBAF5E84F37F8C4525"></a>

##	Partial Index Lookups

When a query references a subset of columns in an index, SnappyData only uses the index for those columns that form a prefix of the indexed columns. For example, consider the index:

``` pre
CREATE INDEX idx ON mytable (col1, col2, col3);
```

SnappyData can use the above index only for search conditions on:

-   col1 only, or
-   col1 and col2, or
-   col1, col2, and col3.

If a query includes search conditions only for col1 and col3, then the index is used only for the col1 search condition, because col1 is a prefix of the indexed columns. The search condition for col3 is treated as a separate, unindexed filter operation.

Create additional indexes when necessary to ensure that queries use a prefix of an index's columns.

<a id="create-index__section_18966FC66980497B864BDDA883DC3464"></a>

##Case Sensitivity for Indexes

By default all indexes are case-sensitive. SnappyData supports case-insensitive indexes for CHAR and VARCHAR columns if you specify the `caseSensitive=false` hint at the end of the `CREATE INDEX` statement. A case insensitive index enables queries to look up column values while ignoring case differences.

SnappyData performs case insensitive index lookups only for equality-based criteria that appear in queries. Case is not ignored when using the LIKE clause, or when using the &gt;, &gt;=, &lt;, or &lt;= operators.

!!! Note: 
	Use the <a href="ref-explain.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref noPageCitation" title="Capture or display the query execution plan for a statement.">EXPLAIN</a> command and view the generated query plan to verify that case insensitive index searches are used where needed. 

For columns that are not part of an index, SnappyData observes case-sensitivity only if it is explicitly required (for example, if the query specifies `UPPER(column-name='UPPERCASE_VALUE'`).

Queries that use the `OR` clause to perform comparisons on columns of a case-sensitive index must use the `UPPER` function on those columns to ensure correct results.

##Example

Create an index on two columns:

``` pre
CREATE INDEX idx ON FLIGHTS (flight_id ASC, segment_number DESC);
```

Create a case-insensitive index:

``` pre
CREATE INDEX idx ON FLIGHTS (flight_id ASC, segment_number DESC) -- GEMFIREXD-PROPERTIES caseSensitive=false <return>
;
```


