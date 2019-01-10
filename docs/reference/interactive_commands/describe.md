# describe

Provides a description of the specified table or view.

## Syntax

```pre
DESCRIBE { table-Name | view-Name }
```

## Description

Provides a description of the specified table or view. For a list of tables in the current schema, use the Show Tables command. For a list of views in the current schema, use the Show Views command. For a list of available schemas, use the Show Schemas command.

If the table or view is in a particular schema, qualify it with the schema name. If the table or view name is case-sensitive, enclose it in single quotes. You can display all the columns from all the tables and views in a single schema in a single display by using the wildcard character '\*'. 

## Example

```pre
snappy> describe maps;
COLUMN_NAME         |TYPE_NAME|DEC&|NUM&|COLUM&|COLUMN_DEF|CHAR_OCTE&|IS_NULL&
------------------------------------------------------------------------------
MAP_ID              |INTEGER  |0   |10  |10    |AUTOINCRE&|NULL      |NO
MAP_NAME            |VARCHAR  |NULL|NULL|24    |NULL      |48        |NO
REGION              |VARCHAR  |NULL|NULL|26    |NULL      |52        |YES
AREA                |DECIMAL  |4   |10  |8     |NULL      |NULL      |NO
PHOTO_FORMAT        |VARCHAR  |NULL|NULL|26    |NULL      |52        |NO
PICTURE             |BLOB     |NULL|NULL|102400|NULL      |NULL      |YES

6 rows selected
snappy>
```


