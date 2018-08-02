# LATERAL VIEW

Support for hive compatible LATERAL VIEW. It works with a table generating function like explode() and for each output row, joins it with the base table to create a view. Refer to [this document](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView) for details.

## Syntax

```pre
FROM baseTable (lateralView)*
lateralView: LATERAL VIEW function([expressions]) tableAlias [AS columnAlias (',' columnAlias)*]

```

## Example

```pre
"id": 1
"purpose": "business"
"type": sales
"contact" : [{ "phone" : "555-1234", "email" : "jsmth@company.com" },
           { "phone" : "666-1234", "email" : "smithj@company.com" } ]

SELECT id, part.phone, part.email FROM json
  LATERAL VIEW explode(parts) partTable AS part

1, "555-1234", "jsmith@company.com"
1, "666-1234", "smithj@company.com"
```
