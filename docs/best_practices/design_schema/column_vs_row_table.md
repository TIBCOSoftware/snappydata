<a id="column-row"></a>
## Using Column vs Row Table

A columnar table data is stored in a sequence of columns, whereas, in a row table it stores table records in a sequence of rows.

<a id="column-table"></a>
### Using Column Tables

**Analytical Queries**: A column table has distinct advantages for OLAP queries and therefore large tables involved in such queries are recommended to be created as columnar tables. These tables are rarely mutated (deleted/updated).
For a given query on a column table, only the required columns are read (since only the required subset columns are to be scanned), which gives a better scan performance. Thus, aggregation queries execute faster on a column table compared  to a  row table.

**Compression of Data**: Another advantage that the column table offers is it allows highly efficient compression of data which reduces the total storage footprint for large tables.

Column tables are not suitable for OLTP scenarios. In this case, row tables are recommended.

<a id="row-table"></a>
### Using Row Tables

**OLTP Queries**: Row tables are designed to return the entire row efficiently and are suited for OLTP scenarios when the tables are required to be mutated frequently (when the table rows need to be updated/deleted based on some conditions). In these cases, row tables offer distinct advantages over the column tables.

**Point queries**: Row tables are also suitable for point queries (for example, queries that select only a few records based on certain where clause conditions). 

**Small Dimension Tables**: Row tables are also suitable to create small dimension tables as these can be created as replicated tables (table data replicated on all data servers).

**Create Index**: Row tables also allow the creation of an index on certain columns of the table which improves  performance.

!!! Note
	In the current release of SnappyData, updates and deletes are not supported on column tables. This feature will be added in a future release.