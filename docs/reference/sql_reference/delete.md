# DELETE

Delete rows from a table.

##Syntax

``` pre
{
    DELETE FROM table-name [ [ AS ] correlation-name ]
        [ WHERE ]
}
```

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_F763D37B83E54D828B8572FF3192C67F"></a>
##Description

This form is called a searched delete, removes all rows identified by the table name and WHERE clause.

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_BAC4132F8CC247AD9959D1656C460D72"></a>

##Example

``` pre
-- Delete rows from the CUSTOMERS table where the CID is equal to 10.
DELETE FROM TRADE.CUSTOMERS WHERE CID = 10;

-- Delete all rows from table T.
DELETE FROM T;
```
!!!Note
	This statement is currently supported only for row tables.

