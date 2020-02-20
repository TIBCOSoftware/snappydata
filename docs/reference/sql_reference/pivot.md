# PIVOT

## Syntax
## Description 

TIBCO ComputeDB parser support for PIVOT clause. It deviates from Spark 2.4 support in that it
only allows literals in the value IN list rather than named expressions. On the contrary, it supports
explicit GROUP BY columns with PIVOT instead of always doing implicit detection (which may
be different from what user needs in some cases).

## Example

Both the following examples are identical. Spark 2.4 does not support second variant:

  ```
select * from (
    select year(day) year, month(day) month, temp
    from dayAvgTemp
  )
  PIVOT (
    CAST(avg(temp) AS DECIMAL(5, 2))
    FOR month IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
  )
  ORDER BY year DESC
```

Compared to blog example the **IN** clause only supports constants and not aliases.

```
  select * from (
    select year(day) year, month(day) month, temp
    from dayAvgTemp
  )
  PIVOT (
    CAST(avg(temp) AS DECIMAL(5, 2))
    FOR month IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
  )
  GROUP BY year
  ORDER BY year DESC

```

