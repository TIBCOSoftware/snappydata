io.snappydata.hydra.snapshotIsolation.SnapshotIsolationPrms-snappyDDLExtn =
" USING row OPTIONS(PERSISTENCE '${persistenceMode}')"
" USING row OPTIONS(PERSISTENCE '${persistenceMode}')"
" USING row OPTIONS(PERSISTENCE '${persistenceMode}')"
" USING row OPTIONS(partition_by 'City,Country', buckets '19', redundancy '${redundantCopies}',PERSISTENCE '${persistenceMode}')"
" USING column OPTIONS(partition_by 'City,Country', buckets '19', colocate_with 'employees',redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
" USING column OPTIONS(partition_by 'OrderID', buckets '13', redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
" USING column OPTIONS(partition_by 'OrderID', buckets '13', COLOCATE_WITH 'orders', redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}',COLUMN_MAX_DELTA_ROWS '100',COLUMN_BATCH_SIZE '20000')"
" USING column OPTIONS(partition_by 'ProductID,SupplierID', buckets '17', redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
" USING column OPTIONS(PARTITION_BY 'SupplierID', buckets '37',redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
" USING column OPTIONS(partition_by 'TerritoryID', buckets '3', redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
" USING row OPTIONS(partition_by 'EmployeeID', buckets '1', redundancy '${redundantCopies}', PERSISTENCE '${persistenceMode}')"
;