io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-snappyDDLExtn =
" USING row OPTIONS(PERSISTENT 'sync')"
" USING row OPTIONS(PERSISTENT 'sync')"
" USING row OPTIONS(PERSISTENT 'sync')"
" USING row OPTIONS(partition_by 'PostalCode,Region', buckets '19', redundancy '1',PERSISTENT 'sync')"
" USING column OPTIONS(partition_by 'PostalCode,Region', buckets '19', colocate_with 'employees',redundancy '1', PERSISTENT 'sync')"
" USING row OPTIONS(partition_by 'OrderID', buckets '13', redundancy '1', PERSISTENT 'sync')"
" USING row OPTIONS(partition_by 'OrderID', buckets '13', COLOCATE_WITH 'orders', redundancy '1', PERSISTENT 'sync')"
" USING column OPTIONS(partition_by 'ProductID,SupplierID', buckets '17', redundancy '1', PERSISTENT 'sync')"
" USING column OPTIONS(PARTITION_BY 'SupplierID', buckets '37',redundancy '1', PERSISTENT 'sync')"
" USING column OPTIONS(partition_by 'TerritoryID', buckets '3', redundancy '1', PERSISTENT 'sync')"
" USING row OPTIONS(partition_by 'TerritoryID', buckets '3', colocate_with 'territories', redundancy '1', PERSISTENT 'sync')"
;