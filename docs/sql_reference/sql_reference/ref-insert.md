# INSERT

An INSERT statement creates a row or rows and stores them in the named table. The number of values assigned in an INSERT statement must be the same as the number of specified or implied columns.

##Syntax

``` pre
INSERT INTO table-name
    [ ( simple-column-name [ , simple-column-name ]* ) ]
   Query
```

<a id="reference_2A553C72CF7346D890FC904D8654E062__section_69794C56F9E840C991CE0B3A699D6013"></a>

##Description

Query can be:

-   a VALUES list
-   a multiple-row VALUES expression

<p class="note"><strong>Note:</strong> RowStore does not support an INSERT with a subselect query if any subselect query requires aggregation. </p>
Single-row and multiple-row lists can include the keyword DEFAULT. Specifying DEFAULT for a column inserts the column's default value into the column. Another way to insert the default value into the column is to omit the column from the column list and only insert values into other columns in the table. More information is provided in <a href="ref-valuesexpression.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref" title="The VALUES expression allows construction of a row or a table from other values.">VALUES</a>.

<a href="ref-select.html#reference_29DE31649A5149C3B89F958FC5CB6CBE" class="xref" title="A query with an optional ORDER BY CLAUSE and an optional FOR UPDATE clause.">SELECT</a> and <a href="ref-join.html#reference_9518856325F74F79B13674B8E060E6C5" class="xref" title="Perform joins between two tables.">JOIN Operations</a> provide additional information.

<a id="reference_2A553C72CF7346D890FC904D8654E062__section_33D88974B03A45EC91226B0D46C14549"></a>

##Statement dependency system

The INSERT statement depends on the table being inserted into, all of the conglomerates (units of storage such as heaps or indexes) for that table, and any other table named in the statement. Any statement that creates or drops an index or a constraint for the target table of a prepared INSERT statement invalidates the prepared INSERT statement.

##Example

``` pre
INSERT INTO TRADE.CUSTOMERS
      VALUES (1, 'J Pearson', '07-06-2002', 'VMWare', 1);

-- Insert a new customer into the CUSTOMERS  table,
-- but do not assign  value to  'SINCE'  column
INSERT INTO TRADE.CUSTOMERS(CID ,CUST_NAME , ADDR ,TID)
 VALUES (1, 'J. Pearson', 'VMWare', 1);

-- Insert two new customers using one statement 
-- into the CUSTOMER table as in the previous example, 
-- but do not assign  value to 'SINCE'  field of the new customer.
INSERT INTO TRADE.CUSTOMERS (CID ,CUST_NAME , ADDR ,TID)
 VALUES (1, 'J. Pearson' , 'VMWare', 1),
 (2, 'David Y.' , 'VMWare', 1);

-- Insert the DEFAULT value for the LOCATION column
INSERT INTO TRADE.CUSTOMERS
      VALUES (1, 'J. Pearson', DEFAULT, 'VMWare',1);

-- Insert using a select statement.
INSERT INTO TRADE.NEWCUSTOMERS
     SELECT * from TRADE.CUSTOMERS WHERE TID=1;
```


