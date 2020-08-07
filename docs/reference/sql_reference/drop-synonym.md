# DROP SYNONYM

```pre
DROP SYNONYM [ IF EXISTS ] [schema-name.]synonym-name;
```

## Description

Removes the specified synonym. Include the `IF EXISTS` clause to execute the statement only if the specified synonym exists in TIBCO ComputeDB. The *schema-name.* prefix is optional if you are currently using the schema that contains the synonym.

## Example

```pre
DROP SYNONYM IF EXISTS app.myairline;
```

**Related Topics**</br>

* [CREATE SYNONYM](create-synonym.md)

