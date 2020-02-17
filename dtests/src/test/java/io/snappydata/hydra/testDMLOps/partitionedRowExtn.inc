io.snappydata.hydra.testDMLOps.SnappySchemaPrms-snappyDDLExtn =
" USING row OPTIONS(PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(partition_by 'OrderID', buckets '13', redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(partition_by 'OrderID', buckets '13', COLOCATE_WITH 'orders', redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(partition_by 'ProductID,SupplierID', buckets '17', redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(PARTITION_BY 'SupplierID', buckets '123',redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(partition_by 'TerritoryID', buckets '3', redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
" USING row OPTIONS(partition_by 'EmployeeID', buckets '1', redundancy '${redundantCopies}', PERSISTENT '${persistenceMode}')"
;

INCLUDE $JTESTS/io/snappydata/hydra/testDMLOps/nwTablesDDLWithPK.inc;