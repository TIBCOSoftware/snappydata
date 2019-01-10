# SYSTABLESTATS

Creates a virtual table that displays statistics for all the tables that exist in distributed system.

|Column Name|Type|Length|Nullable|Contents|
|---|---|---|---|----|
|TABLE |VARCHAR|512|NO|Name of the table|
|IS_COLUMN_TABLE|BOOLEAN||NO|Displays **True** for column tables and **False** for row tables|
|IS_REPLICATED_TABLE|BOOLEAN||NO|Displays **True** for replicated tables and **False** for partitioned tables|
|ROW_COUNT|BIGINT|10|NO|Total number of rows in the table|
|SIZE_IN_MEMORY|BIGINT|10|NO|Heap memory used by data table to store its data/records.|
|TOTAL_SIZE|BIGINT|10|NO|Collective physical memory and disk overflow space used by the data table to store its data/record|
|BUCKETS|BIGINT|10|NO|Number of buckets|

**Example** </br>
```pre
snappy> select * from SYS.TABLESTATS;
TABLE         |IS_COLUMN_TABLE|IS_REPLICATED_TABLE|ROW_COUNT|SIZE_IN_MEMORY|TOTAL_SIZE|BUCKETS
----------------------------------------------------------------------------------------------- 
APP.CUSTOMERS |false          |true               |0        |1672          |1672      |1
APP.ABCD      |true           |false              |7        |36704         |36704     |10
TRADE.ORDERS  |false          |false              |1        |199968        |199968    |113
APP.EMPLOYEE  |true           |false              |10       |37280         |37280     |10

4 rows selected
```
