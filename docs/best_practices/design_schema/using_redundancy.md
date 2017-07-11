<a id="redundancy"></a>
## Using Redundancy

REDUNDANCY clause of [CREATE TABLE](../reference/sql_reference/create-table.md) specifies the number of secondary copies you want to maintain for your partitioned table. This allows the table data to be highly available even if one of the SnappyData members fails or shuts down. 

A REDUNDANCY value of 1 is recommended to maintain a secondary copy of the table data. A large value for REDUNDANCY clause has an adverse impact on performance, network usage, and memory usage.

For an example of the REDUNDANCY clause refer to [Tables in SnappyData](../programming_guide.md#tables-in-snappydata).

