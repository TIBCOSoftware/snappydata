#PUT INTO

Creates or replaces rows in a table without first checking existing primary key values. If existing rows with the same primary key are present in the table, they are overwritten with the new values. This syntax can be used to speed insert operations when importing data. It is also helpful when working with HDFS-persistent tables where checking primary key values requires data to be retrieved from HDFS.

##Syntax

``` pre
PUT INTO table-name
    { VALUES ( column-value [ , column-value ]* ) |
   Query
    }
```

<a id="reference_2A553C72CF7346D890FC904D8654E062__section_69794C56F9E840C991CE0B3A699D6013"></a>
##Description

PUT INTO speeds insert operations on HDFS read/write tables by ignoring existing primary key values. PUT INTO uses a syntax similar to the INSERT statement, but RowStore does not check existing primary key values before executing the PUT INTO command. If a row with the same pprimary key exists in the table, PUT INTO simply overwrites the older row value. If no rows with the same primary key exist, PUT INTO operates like a standard INSERT. This behavior ensures that only the last primary key value inserted or updated remains in the system, which preserves the primary key constraint. Removing the primary key check speeds execution when importing bulk data, or when updating a large number of rows in HDFS stores.

The PUT INTO statement is similar to the "UPSERT" command or capability provided by other RDBMS to relax primary key checks. By default the PUT INTO statement ignores only primary key constraints. All other column constraints (unique, check, and foreign key) are honored unless you explicitly set the <a href="../configuration/ConnectionAttributes.html#jdbc_connection_attributes__secskipconstraintchecks" class="xref noPageCitation">skip-constraint-checks</a> connection property.

!!!Note: 
	RowStore does not support a PUT INTO with a subselect query if any subselect query requires aggregation. </br> RowStore does not support using triggers in combination with the PUT INTO DML syntax. A PUT INTO operation does not invoke triggers. In addition, you cannot use PUT INTO in the triggered SQL statement of a RowStore trigger. See <a href="ref-create-trigger.html#reference_85BF6CBEC8D447989DC8EDE24C9DBFA5" class="xref" title="A trigger defines a set of actions that are executed when a delete, insert, or update operation is performed on a table. For example, if you define a trigger for a delete on a particular table, the trigger&#39;s action occurs whenever someone deletes a row or rows from the table.">CREATE TRIGGER</a>.

##Example

``` pre
-- Insert a new row, or update an existing row with the provided values (ignoring any primary key constraints).
PUT INTO TRADE.HDFSCUSTOMERS
      VALUES (1, 'J Pearson', '07-06-2002', 'VMWare', 1);

-- Insert using a select statement, overwriting rows if necessary.
PUT INTO TRADE.HDFSNEWCUSTOMERS
     SELECT * from TRADE.CUSTOMERS WHERE TID=1;
```


