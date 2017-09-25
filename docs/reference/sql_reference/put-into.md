# PUT INTO

!!!Note:
	* PUT INTO is not supported for column tables
		
	* SnappyData does not support PUT INTO with a subselect query, if, the subselect query requires aggregation

``` bash
PUT INTO table-name
     VALUES ( column-value [ , column-value ]* ) 
```

``` bash
PUT INTO table-name
    ( simple-column-name [ , simple-column-name ]* )
   Query
```
## Description

PUT INTO operates like standard [INSERT](insert.md) statement.

###	For Row Tables

PUT INTO uses a syntax similar to the INSERT statement, but SnappyData does not check existing primary key values before executing the PUT INTO command. If a row with the same primary key exists in the table, PUT INTO simply overwrites the older row value. If no rows with the same primary key exist, PUT INTO operates like a standard INSERT. This behavior ensures that only the last primary key value inserted or updated remains in the system, which preserves the primary key constraint. Removing the primary key check speeds execution when importing bulk data.

The PUT INTO statement is similar to the "UPSERT" command or capability provided by other RDBMS to relax primary key checks. By default, the PUT INTO statement ignores only primary key constraints. <!--All other column constraints (unique, check, and foreign key) are honored unless you explicitly set the [skip-constraint-checks](../../reference/configuration_parameters/skip-constraint-checks.md) connection property.-->

## Example

```pre
PUT INTO TRADE.CUSTOMERS
      VALUES (1, 'User 1', '07-06-2002', 'SnappyData', 1);
```

When specifying columns with table, columns should not have any [CONSTRAINT](create-table.md#constraint), as explained in the following example:

```pre
PUT INTO TRADE.CUSTOMERS (CID ,CUST_NAME , ADDR ,TID)
 VALUES (1, 'User 1' , 'SnappyData', 1),
 (2, 'User 2' , 'SnappyData', 1);
```
