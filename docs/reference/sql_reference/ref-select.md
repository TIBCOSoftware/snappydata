#SELECT

A query with an optional ORDER BY CLAUSE and an optional FOR UPDATE clause.

##Syntax

``` pre
Query [ ORDER BY ] [ FOR UPDATE ]
```

<a id="reference_29DE31649A5149C3B89F958FC5CB6CBE__section_770EC1560B2E4AE3A6534BF9A1C91050"></a>
##Description


A SELECT statement consists of a query with an optional ORDER BY CLAUSE and an optional FOR UPDATE clause. The SELECT statement is so named because the typical first word of the query construct is SELECT. (Query includes the VALUES expression as well as SELECT expressions).

The ORDER BY CLAUSE guarantees the ordering of the ResultSet. The FOR UPDATE clause acquires a lock on the selected rows and makes the result set's cursor updatable.

A SELECT statement returns a ResultSet. A cursor is a pointer to a specific row in ResultSet. In Java applications, all ResultSets have an underlying associated SQL cursor, often referred to as the result set's cursor. The cursor can be updatable, that is, you can update or delete rows as you step through the ResultSet if the SELECT statement that generated it and its underlying query meet cursor updatability requirements, as detailed below. In RowStore, the FOR UPDATE clause is required in order to obtain an updatable cursors, or to limit the columns that can be updated. RowStore also requires that you use a peer client connection in order to obtain an updateable result set. See <a href="ref-sql-limitations.html#concept_05E66BCA75DD4940994906F0BF31AE17__section_315F5E47AF084F0AB987FF3812B9C70E" class="xref">Updatable Result Sets Limitations</a> for more information about result set limitations.

!!!Note: 
	The ORDER BY clause allows you to order the results of the SELECT. Without the ORDER BY clause, the results are returned in random order. Unlike in some other databases, the select results have no relation with the insertion order of those rows in the table. </br>
Explicit naming of a cursor is not supported.

<a id="reference_29DE31649A5149C3B89F958FC5CB6CBE__sec_refupdateable"></a>

Requirements for updatable cursors and updatable ResultSets


Only simple, single-table SELECT cursors can be updatable. The SELECT statement for updatable ResultSets has the same syntax as the SELECT statement for updatable cursors. To generate updatable cursors:

-   You must issue the SELECT statement using a RowStore peer client connection.
-   The SELECT statment must include the FOR UPDATE clause.
-   The SELECT statement must not include an ORDER BY clause.
-   The underlying Query must be a <a href="ref-selectexpression.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref noPageCitation" title="A SelectExpression is the basic SELECT-FROM-WHERE construct used to build a table value based on filtering and projecting values from other tables.">SelectExpression</a>.
-   The <a href="ref-selectexpression.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref noPageCitation" title="A SelectExpression is the basic SELECT-FROM-WHERE construct used to build a table value based on filtering and projecting values from other tables.">SelectExpression</a> in the underlying Query must not include:
    -   DISTINCT
    -   Aggregates
    -   GROUP BY clause
    -   HAVING clause
    -   ORDER BY clause
-   The FROM clause in the underlying Query must not have:
    -   more than one table in its FROM clause
    -   anything other than one table name
    -   <a href="ref-selectexpression.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref noPageCitation" title="A SelectExpression is the basic SELECT-FROM-WHERE construct used to build a table value based on filtering and projecting values from other tables.">SelectExpression</a>s
    -   subqueries

!!!Note:
	Cursors are read-only by default. To produce an updatable cursor besides meeting the requirements listed above, the concurrency mode for the ResultSet must be ResultSet.CONCUR\_UPDATABLE (see <a href="ref-for-update.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref">FOR UPDATE</a>).</p>

##Statement dependency system

The SELECT depends on all the tables and views named in the query and the conglomerates (units of storage such as heaps and indexes) chosen for access paths on those tables.

CREATE INDEX does not invalidate a prepared SELECT statement.

A DROP INDEX statement invalidates a prepared SELECT statement if the index is an access path in the statement. If the SELECT includes views, it also depends on the dictionary objects on which the view itself depends (see CREATE VIEW statement).

Any prepared UPDATE WHERE CURRENT or DELETE WHERE CURRENT statement against a cursor of a SELECT depends on the SELECT. Removing a SELECT through a java.sql.Statement.close request invalidates the UPDATE WHERE CURRENT or DELETE WHERE CURRENT.

The SELECT depends on all synonyms used in the query. Dropping a synonym invalidates a prepared SELECT statement if the statement uses the synonym.

##Example

``` pre
-- lists the customer ID and expression 
-- LOANLIMIT-AVAILLOAN as REMAINING_LIMIT and
-- orders by the new name REMAINING_LIMIT
SELECT CID, LOANLIMIT-AVAILLOAN AS REMAINING_LIMIT
     FROM TRADE.NETWORTH  ORDER BY REMAINING_LIMIT

-- creating an updatable cursor with a FOR UPDATE clause 
-- to update the SINCE date 
-- column in the CUSTOMERS table
SELECT SINCE FROM TRADE.CUSTOMERS  FOR UPDATE OF SINCE
```


