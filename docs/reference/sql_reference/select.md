# SELECT

```pre
SELECT [DISTINCT] named_expression[, named_expression, ...]
    FROM relation[, relation, ...]
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
    | (table_name|query) [sample] [AS alias]

expressions:
    : expression[, expression, ...]

sort_expressions:
    : expression [ASC|DESC][, expression [ASC|DESC], ...]
```

For information on executing queries on Synopsis Data Engine, refer to [SDE](/../../aqp.md).
## Description

Output data from one or more relations.

A relation here refers to any source of input data. It could be the contents of an existing table (or view), the joined result of two existing tables, or a subquery (the result of another select statement).

`DISTINCT`</br>
Select all matching rows from the relation then remove duplicate results.

`WHERE`</br>
Filter rows by a predicate.

`HAVING`</br>
Filter grouped result by a predicate.

`ORDER BY`</br>
Impose total ordering on a set of expressions. Default sort direction is ascending. This may not be used with SORT BY, CLUSTER BY, or DISTRIBUTE BY.

`DISTRIBUTE BY`</br>
Repartition rows in the relation based on a set of expressions. Rows with the same expression values will be hashed to the same worker. This may not be used with ORDER BY or CLUSTER BY.

`SORT BY`</br>
Impose ordering on a set of expressions within each partition. Default sort direction is ascending. This may not be used with ORDER BY or CLUSTER BY.

`CLUSTER BY`</br>
Repartition rows in the relation based on a set of expressions and sort the rows in ascending order based on the expressions. In other words, this is a shorthand for DISTRIBUTE BY and SORT BY where all expressions are sorted in ascending order. This may not be used with ORDER BY, DISTRIBUTE BY, or SORT BY.

`WINDOW`</br>
Assign an identifier to a window specification.

`LIMIT`</br>
Limit the number of rows returned.


## Example

```pre
    SELECT * FROM boxes
    SELECT width, length FROM boxes WHERE height=3
    SELECT DISTINCT width, length FROM boxes WHERE height=3 LIMIT 2
    SELECT * FROM VALUES (1, 2, 3) AS (width, length, height)
    SELECT * FROM VALUES (1, 2, 3), (2, 3, 4) AS (width, length, height)
    SELECT * FROM boxes ORDER BY width
    SELECT * FROM boxes DISTRIBUTE BY width SORT BY width
    SELECT * FROM boxes CLUSTER BY length
```

JOINS
-----------

```pre
    join_relation:
        | relation join_type JOIN relation (ON boolean_expression | USING (column_name[, column_name, ...]))
        : relation NATURAL join_type JOIN relation
    join_type:
        | INNER
        | (LEFT|RIGHT) SEMI
        | (LEFT|RIGHT|FULL) [OUTER]
        : [LEFT] ANTI
```

`INNER JOIN`</br>
Select all rows from both relations where there is match.

`OUTER JOIN`</br>
Select all rows from both relations, filling with null values on the side that does not have a match.

`SEMI JOIN`</br>
Select only rows from the side of the SEMI JOIN where there is a match. If one row matches multiple rows, only the first match is returned.

`LEFT ANTI JOIN`</br>
Select only rows from the left side that match no rows on the right side.

**Example**:

```pre
    SELECT * FROM boxes INNER JOIN rectangles ON boxes.width = rectangles.width
    SELECT * FROM boxes FULL OUTER JOIN rectangles USING (width, length)
    SELECT * FROM boxes NATURAL JOIN rectangles
```

AGGREGATION
------------

```pre
    aggregation:
        : GROUP BY expressions [(WITH ROLLUP | WITH CUBE | GROUPING SETS (expressions))]
```

Group by a set of expressions using one or more aggregate functions. Common built-in aggregate functions include count, avg, min, max, and sum.

`ROLLUP`</br>
Create a grouping set at each hierarchical level of the specified expressions. For instance, For instance, GROUP BY a, b, c WITH ROLLUP is equivalent to GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (a), ()). The total number of grouping sets will be N + 1, where N is the number of group expressions.

`CUBE`</br>
Create a grouping set for each possible combination of a set of the specified expressions. For instance, GROUP BY a, b, c WITH CUBE is equivalent to GROUP BY a, b, c GROUPING SETS ((a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ()). The total number of grouping sets will be 2^N, where N is the number of group expressions.

`GROUPING SETS`</br>
Perform a group by for each subset of the group expressions specified in the grouping sets. For instance, GROUP BY x, y GROUPING SETS (x, y) is equivalent to the result of GROUP BY x unioned with that of GROUP BY y.

**Example**:

```pre
    SELECT height, COUNT(*) AS num_rows FROM boxes GROUP BY height
    SELECT width, AVG(length) AS average_length FROM boxes GROUP BY width
    SELECT width, length, height FROM boxes GROUP BY width, length, height WITH ROLLUP
    SELECT width, length, avg(height) FROM boxes GROUP BY width, length GROUPING SETS (width, length)
```

Window Functions
--------------

```pre
window_expression:
    : expression OVER window_spec

named_window:
    : window_identifier AS window_spec

window_spec:
    | window_identifier
    : ((PARTITION|DISTRIBUTE) BY expressions
          [(ORDER|SORT) BY sort_expressions] [window_frame])

window_frame:
    | (RANGE|ROWS) frame_bound
    : (RANGE|ROWS) BETWEEN frame_bound AND frame_bound

frame_bound:
    | CURRENT ROW
    | UNBOUNDED (PRECEDING|FOLLOWING)
    : expression (PRECEDING|FOLLOWING)
```

Compute a result over a range of input rows. A windowed expression is specified using the OVER keyword, which is followed by either an identifier to the window (defined using the WINDOW keyword) or the specification of a window.

`PARTITION BY`</br>
Specify which rows will be in the same partition, aliased by DISTRIBUTE BY.

`ORDER BY`</br>
Specify how rows within a window partition are ordered, aliased by SORT BY.

`RANGE bound`</br>
Express the size of the window in terms of a value range for the expression.

`ROWS bound`</br>
Express the size of the window in terms of the number of rows before and/or after the current row.

`CURRENT ROW`</br>
Use the current row as a bound.

`UNBOUNDED`</br>
Use negative infinity as the lower bound or infinity as the upper bound.

`PRECEDING`</br>
If used with a RANGE bound, this defines the lower bound of the value range. If used with a ROWS bound, this determines the number of rows before the current row to keep in the window.

`FOLLOWING`</br>
If used with a RANGE bound, this defines the upper bound of the value range. If used with a ROWS bound, this determines the number of rows after the current row to keep in the window.
