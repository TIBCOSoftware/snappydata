# EXPLAIN

## Syntax

    snappy> explain select <query>


## Description 

Provides information about how your query is executed.

## Example

        snappy> explain select avg(arrdelay) from airline;
        plan                                                                                                                                                                                                                                                                                                                            
       --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        == Physical Plan ==
        CollectAggregate
        +- *SnappyHashAggregate(ByteBufferHashMap used = false; keys=[], modes=Partial, functions=[partial_avg(cast(arrdelay#750 as bigint))])
           +- *Partitioned Scan ColumnFormatRelation[app.airline], Requested Columns = [arrdelay#750] partitionColumns = [] numBuckets = 8 numPartitions = 2
