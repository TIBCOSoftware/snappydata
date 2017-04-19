#SELECT

##Syntax
```
SELECT [ALL|DISTINCT] named_expression[, named_expression, ...]
    FROM relation[, relation, ...]
    [lateral_view[, lateral_view, ...]]
    [WHERE boolean_expression]
    [aggregation [HAVING boolean_expression]]
    [ORDER BY sort_expressions]
    [CLUSTER BY expressions]
    [DISTRIBUTE BY expressions]
    [SORT BY sort_expressions]
    [WINDOW named_window[, WINDOW named_window, ...]]
    [LIMIT num_rows]

named_expression:
    : expression [AS alias]

relation:
    | join_relation
    | (table_name|query|relation) [sample] [AS alias]
    : VALUES (expressions)[, (expressions), ...]
          [AS (column_name[, column_name, ...])]

expressions:
    : expression[, expression, ...]

sort_expressions:
    : expression [ASC|DESC][, expression [ASC|DESC], ...]

```

##Description

Output data from one or more relations.

A relation here refers to any source of input data. It could be the contents of an existing table (or view), the joined result of two existing tables, or a subquery (the result of another select statement).

**ALL**

Select all matching rows from the relation. Enabled by default.

**DISTINCT**

Select all matching rows from the relation then remove duplicate results.

**WHERE**

Filter rows by predicate.

**HAVING**

Filter grouped result by predicate.

**ORDER BY**

Impose total ordering on a set of expressions. Default sort direction is ascending. This may not be used with SORT BY, CLUSTER BY, or DISTRIBUTE BY.

**DISTRIBUTE BY** 

Repartition rows in the relation based on a set of expressions. Rows with the same expression values will be hashed to the same worker. This may not be used with ORDER BY or CLUSTER BY.

**SORT BY** 

Impose ordering on a set of expressions within each partition. Default sort direction is ascending. This may not be used with ORDER BY or CLUSTER BY.

**CLUSTER BY** 

Repartition rows in the relation based on a set of expressions and sort the rows in ascending order based on the expressions. In other words, this is a shorthand for DISTRIBUTE BY and SORT BY where all expressions are sorted in ascending order. This may not be used with ORDER BY, DISTRIBUTE BY, or SORT BY.

**WINDOW**

Assign an identifier to a window specification (more details below).

**LIMIT**

Limit the number of rows returned.

**VALUES**

Explicitly specify values instead of reading them from a relation.

##Example

```


