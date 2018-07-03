# PUT INTO

<note>
	 Insert/PUT INTO with partial column specification is not currently supported.

```pre
    PUT INTO table-name VALUES ( column-value [ , column-value ]* ) 
```

```pre
    PUT INTO table-name
        ( simple-column-name [ , simple-column-name ]* )
       Query
```
</note>

## Description

PUT INTO operates like standard [INSERT](insert.md) statement.

###	For Row Tables

PUT INTO uses a syntax similar to the INSERT statement, but SnappyData does not check existing primary key values before executing the PUT INTO command. If a row with the same primary key exists in the table, PUT INTO simply overwrites the older row value. If no rows with the same primary key exist, PUT INTO operates like a standard INSERT. This behavior ensures that only the last primary key value inserted or updated remains in the system, which preserves the primary key constraint. Removing the primary key check speeds execution when importing bulk data.

The PUT INTO statement is similar to the "UPSERT" command or capability provided by other RDBMS to relax primary key checks. By default, the PUT INTO statement ignores only primary key constraints. <!--All other column constraints (unique, check, and foreign key) are honored unless you explicitly set the [skip-constraint-checks](../../reference/configuration_parameters/skip-constraint-checks.md) connection property.-->

#### Example

```pre
PUT INTO TRADE.CUSTOMERS VALUES (1, 'User 1', '2001-10-12', 'SnappyData', 1);
```

When specifying columns with table, columns should not have any [CONSTRAINT](create-table.md#constraint), as explained in the following example:

```pre
PUT INTO TRADE.CUSTOMERS (CID ,CUST_NAME , ADDR ,TID) VALUES (1, 'User 1' , 'SnappyData', 1), (2, 'User 2' , 'SnappyData', 1);
```

PUT into another table using a select statement
```pre
PUT INTO TRADE.NEWCUSTOMERS SELECT * from TRADE.CUSTOMERS;

PUT INTO TRADE.NEWCUSTOMERS SELECT * from TRADE.CUSTOMERS WHERE CUST_NAME='User 1'
```

###	For Column Tables

If you need the `putInto()` functionality for column tables, you must specify key_columns while defining the column table.
PUT INTO updates the row if present, else, it inserts the row. </br>
As column tables do not have primary key functionality you must specify key_columns when creating the table.</br>
These columns are used to identify a row uniquely. PUT INTO is available by SQL as well APIs.


#### Example

**For SQL**

```pre
// Insert into another table using a select statement for column tables with key columns
PUT INTO NEWEMPLOYEES SELECT * from EMPLOYEES;

PUT INTO NEWEMPLOYEES SELECT * from EMPLOYEES WHERE C_NAME='User 1'
```

**For API**

API is available from the DataFrameWriter extension.

```pre
import org.apache.spark.sql.snappy._
dataFrame.write.putInto("col_table")
```
