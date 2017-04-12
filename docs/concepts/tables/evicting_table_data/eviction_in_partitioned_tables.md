# Eviction in Partitioned Tables
In partitioned tables, SnappyData removes the oldest entry it can find in the bucket where the new entry operation is being performed. SnappyData maintains LRU entry information on a bucket-by-bucket basis, because the performance cost of maintaining information across the entire partitioned table is too great.

SnappyData chooses buckets for LRU eviction in these ways:

* For memory and entry count eviction, LRU eviction is done in the bucket where the new row is being added until the overall size of the combined buckets in the member has dropped enough to perform the operation without going over the limit.

* For heap eviction, each partitioned table bucket is treated as if it were a separate table, with each eviction action only considering the LRU for the bucket, and not the partitioned table as a whole.

Eviction in a partitioned table may leave older entries for the table in other buckets in the local data store as well as in other hosts in the distributed system. It may also leave entries in a primary copy that it evicts from a secondary copy or vice-versa. However, SnappyData synchronizes the redundant copies when an entry with the same primary key is inserted or updated.