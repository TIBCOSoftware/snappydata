# CREATE SYNONYM
Provide an alternate name for a table or view.

```pre
snappy> CREATE SYNONYM [synonym-name] FOR [view-name | table-name];
```

## Description
The synonym-Name in the statement represents the synonym name you are giving the target table or view, while the view-Name or table-Name represents the original name of the target table or view.

Synonyms for other synonyms can also be created, resulting in nested synonyms. A synonym can be used instead of the original qualified table or view name in SELECT, INSERT, UPDATE, DELETE or LOCK TABLE statements. Synonym for a table or a view can be created for tables or views that doesn't exist, but the target table or view must be present before the synonym can be used.

Synonyms share the same namespace as tables or views. Synonyms name cannot be same as the name of an already existing table/view.

A synonym can be defined for a table/view that does not exist when you create the synonym. If the table or view doesn't exist, you will receive a warning message (SQLSTATE 01522). The referenced object must be present when the synonym is used in a DML statement.

Nested synonym can be created(a synonym for another synonym), but any attempt to create a synonym that results in a circular reference will return an error message (SQLSTATE 42916).

Synonyms cannot be defined in system schemas. All schemas starting with 'SYS' are considered system schemas and are reserved by TIBCO ComputeDB.

A synonym cannot be defined on a temporary table. Attempting to define a synonym on a temporary table will return an error message (SQLSTATE XCL51).

## Example

```pre
snappy> CREATE SYNONYM myairline FOR airlineREF;
```

**Related Topics**</br>

* [DROP SYNONYM](drop-synonym.md)

* [CREATE VIEW](create-view.md)
