# CREATE INDEX

Creates an index on one or more columns of a table.

##Syntax

``` pre
CREATE INDEX index_name
    ON table-name (
    column-name 
    [ , column-name] * ) 
```

##Description

The `CREATE INDEX` statement creates an index on one or more columns of a table. Indexes can speed up queries that use those columns for filtering data, or can also enforce a unique constraint on the indexed columns.

##Example

Create an index on two columns:

``` pre
CREATE INDEX idx ON FLIGHTS (flight_id, segment_number);
```

