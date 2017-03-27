# DROP TABLE

Remove the specified table.

##Syntax

``` pre
DROP TABLE [ IF EXISTS ] [schema-name.]table-name
```

<a id="reference_3D95C4FE18FC4A81BBA397AB8BEFE711__section_CC9494F3E8F44B6A96E07EB9D38B9A8A"></a>
##Description

Removes the specified table. Include the `IF EXISTS` clause to execute the statement only if the specified table exists in SnappyData. The *schema-name.* prefix is optional if you are currently using the schema that contains the table.

!!!Note:
	Triggers, constraints (primary, unique, check and references from the table being dropped) and indexes on the table are silently dropped. Dropping a table invalidates statements that depend on the table. </p>
