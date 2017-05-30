#SELECT

##Syntax
```
SELECT [DISTINCT] named_expression[, named_expression, ...]
    FROM relation[, relation, ...]
    [lateral_view[, lateral_view, ...]]
    [WHERE boolean_expression]
    [aggregation [HAVING boolean_expression]]
    [ORDER BY sort_expressions]
    [CLUSTER BY expressions]
    [DISTRIBUTE BY expressions]
    [SORT BY sort_expressions]
    [LIMIT num_rows]

named_expression:
    : expression [AS alias]

relation:
    | join_relation
    | (table_name|query) [sample] [AS alias]

expressions:
    : expression[, expression, ...]

sort_expressions:
    : expression [ASC|DESC][, expression [ASC|DESC], ...]

```

##Description

Output data from one or more relations.

A relation here refers to any source of input data. It could be the contents of an existing table (or view), the joined result of two existing tables, or a subquery (the result of another select statement).

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

**LIMIT**

Limit the number of rows returned.

**VALUES**

Explicitly specify values instead of reading them from a relation.

**JOINS**

    join_relation:
        | relation join_type JOIN relation (ON boolean_expression | USING (column_name[, column_name, ...]))
        : relation NATURAL join_type JOIN relation
    join_type:
        | INNER
        | (LEFT|RIGHT) SEMI
        | (LEFT|RIGHT|FULL) [OUTER]
        : [LEFT] ANTI

* INNER JOIN:
Select all rows from both relations where there is match.

* OUTER JOIN:
Select all rows from both relations, filling with null values on the side that does not have a match.

* SEMI JOIN :
Select only rows from the side of the SEMI JOIN where there is a match. If one row matches multiple rows, only the first match is returned.

* LEFT ANTI JOIN: 
Select only rows from the left side that match no rows on the right side.

## Example:

```
    SELECT * FROM boxes INNER JOIN rectangles ON boxes.width = rectangles.width
    SELECT * FROM boxes FULL OUTER JOIN rectangles USING (width, length)
    SELECT * FROM boxes NATURAL JOIN rectangles
```

**AGGREGATION**

```
    aggregation:
        : GROUP BY expressions [(WITH ROLLUP | WITH CUBE | GROUPING SETS (expressions))]
```
Group by a set of expressions using one or more aggregate functions. Common built-in aggregate functions include count, avg, min, max, and sum.

* ROLLUP
Create a grouping set at each hierarchical level of the specified expressions. For instance, For instance, GROUP BY a, b, c WITH ROLLUP is equivalent to GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (a), ()). The total number of grouping sets will be N + 1, where N is the number of group expressions.

* CUBE
Create a grouping set for each possible combination of set of the specified expressions. For instance, GROUP BY a, b, c WITH CUBE is equivalent to GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ()). The total number of grouping sets will be 2^N, where N is the number of group expressions.

* GROUPING SETS
Perform a group by for each subset of the group expressions specified in the grouping sets. For instance, GROUP BY x, y GROUPING SETS (x, y) is equivalent to the result of GROUP BY x unioned with that of GROUP BY y.

**Example**:

```
    SELECT height, COUNT(*) AS num_rows FROM boxes GROUP BY height
    SELECT width, AVG(length) AS average_length FROM boxes GROUP BY width
    SELECT width, length, height FROM boxes GROUP BY width, length, height WITH ROLLUP
    SELECT width, length, avg(height) FROM boxes GROUP BY width, length GROUPING SETS (width, length)
```