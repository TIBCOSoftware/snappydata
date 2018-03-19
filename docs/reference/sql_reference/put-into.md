# PUT INTO

!!!Note
	SnappyData does not support PUT INTO with a subselect query, if, the subselect query requires aggregation.

```no-highlight
PUT INTO table-name
     VALUES ( column-value [ , column-value ]* ) 
```

```no-highlight
PUT INTO table-name
    ( simple-column-name [ , simple-column-name ]* )
   Query
```
</note>

## Description

PUT INTO operates like standard [INSERT](insert.md) statement.

### For Row Tables

PUT INTO uses a syntax similar to the INSERT statement, but SnappyData does not check existing primary key values before executing the PUT INTO command. If a row with the same primary key exists in the table, PUT INTO simply overwrites the older row value. If no rows with the same primary key exist, PUT INTO operates like a standard INSERT. This behavior ensures that only the last primary key value inserted or updated remains in the system, which preserves the primary key constraint. Removing the primary key check speeds execution when importing bulk data.

The PUT INTO statement is similar to the "UPSERT" command or capability provided by other RDBMS to relax primary key checks. By default, the PUT INTO statement ignores only primary key constraints. <!--All other column constraints (unique, check, and foreign key) are honored unless you explicitly set the [skip-constraint-checks](../../reference/configuration_parameters/skip-constraint-checks.md) connection property.-->

#### Example

```no-highlight
PUT INTO TRADE.CUSTOMERS
      VALUES (1, 'User 1', '07-06-2002', 'SnappyData', 1);
```

When specifying columns with table, columns should not have any [CONSTRAINT](create-table.md#constraint), as explained in the following example:

```no-highlight
PUT INTO TRADE.CUSTOMERS (CID ,CUST_NAME , ADDR ,TID)
 VALUES (1, 'User 1' , 'SnappyData', 1),
 (2, 'User 2' , 'SnappyData', 1);
```
### For Column Tables

If you need the `putInto()` functionality for column tables, you must specify key_columns while defining the column table.
PUT INTO updates the row if present, else, it inserts the row. </br>
As column tables do not have primary key functionality you must specify key_columns when creating the table.</br>
These columns are used to identify a row uniquely. PUT INTO is available by SQL as well APIs.


#### Example

**For SQL**

```no-highlight
PUT INTO table col_table select * from row_table
```

**For API**

API is available from the DataFrameWriter extension.

```no-highlight
import org.apache.spark.sql.snappy._
dataFrame.write.putInto("col_table")
```