//set table colomun info for each used by index creation, need only one thread to set in the BB
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_setTableCols
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createIndex
	threadGroups = accessorThreads, ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createIndex
	threadGroups = accessorThreads, ddlThread;

TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createIndex
	 threadGroups = accessorThreads, ddlThread;

sql.SQLPrms-createIndex = true;
sql.SQLPrms-dropIndex = ${dropIndex};
