# ODBC Supported APIs

The following APIs are supported for ODBC in Snappy Driver:

*	[Function APIs](#functionconfr)
*	[Attribute APIs](attriconf)

<a id="functionconfr"></a>
## Function Conformance of ODBC Supported APIs in Snappy Driver

| Function            | Conformance </br>level | Supported in </br>Snappy Driver |Exceptions|
|---------------------|-------------------|----------------------------|--|
| SQLAllocHandle      | Core              | Yes                        ||
| SQLBindCol          | Core              | Yes                        ||
| SQLBindParameter    | Core[1]           | Yes                        ||
| SQLBrowseConnect    | Level 1           | Not                        ||
| SQLBulkOperations   | Level 1           | Yes                        ||
| SQLCancel           | Core[1]           | Yes                        ||
| SQLCloseCursor      | Core              | Yes                        ||
| SQLColAttribute     | Core[1]           | Yes                        ||
| SQLColumnPrivileges | Level 2           | Yes                        ||
| SQLColumns          | Core              | Yes                        ||
| SQLConnect          | Core              | Yes                        ||
| SQLCopyDesc         | Core              | Not                        ||
| SQLDataSources      | Core              | Not                        |As per MSDN document it should implement by Driver Manager.|
| SQLDescribeCol      | Core[1]           | Yes                        ||
| SQLDescribeParam    | Level 2           | Yes                        ||
| SQLDisconnect       | Core              | Yes                        ||
| SQLDriverConnect    | Core              | Yes                        ||
| SQLDrivers          | Core              | Not                        ||
| SQLEndTran          | Core[1]           | Yes                        ||
| SQLExecDirect       | Core              | Yes                        ||
| SQLExecute          | Core              | Yes                        ||
| SQLFetch            | Core              | Yes                        ||
| SQLFetchScroll      | Core[1]           | Yes                        ||
| SQLForeignKeys      | Level 2           | Yes                        ||
| SQLFreeHandle       | Core              | Yes                        ||
| SQLFreeStmt         | Core              | Yes                        ||
| SQLGetConnectAttr   | Core              | Yes                        ||
| SQLGetCursorName    | Core              | Yes                        ||
| SQLGetData          | Core              | Yes                        ||
| SQLGetDescField     | Core              | Not                        ||
| SQLGetDescRec       | Core              | Not                        ||
| SQLGetDiagField     | Core              | Yes                        ||
| SQLGetDiagRec       | Core              | Yes                        ||
| SQLGetEnvAttr       | Core              | Yes                        ||
| SQLGetFunctions     | Core              | Yes                        ||
| SQLGetInfo          | Core              | Yes                        |SQL_DEFAULT_TXN_ISOLATION - **not supported**</br> SQL_TXN_ISOLATION_OPTION -**not supported**</br> SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1 **supported**</br> SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2 **supported**|
| SQLGetStmtAttr      | Core              | Yes                        ||
| SQLGetTypeInfo      | Core              | Yes                        |Supports call only with SQL_ALL_TYPES info type parameter|
| SQLMoreResults      | Level 1           | Yes                        ||
| SQLNativeSql        | Core              | Yes                        ||
| SQLNumParams        | Core              | Yes                        ||
| SQLNumResultCols    | Core              | Yes                        ||
| SQLParamData        | Core              | Yes                        ||
| SQLPrepare          | Core              | Yes                        ||
| SQLPrimaryKeys      | Level 1           | Yes                        ||
| SQLProcedureColumns | Level 1           | Yes                        ||
| SQLProcedures       | Level 1           | Yes                        ||
| SQLPutData          | Core              | Yes                        ||
| SQLRowCount         | Core              | Yes                        ||
| SQLSetConnectAttr   | Core[2]           | Yes                        ||
| SQLSetCursorName    | Core              | Yes                        ||
| SQLSetDescField     | Core[1]           | Not                        ||
| SQLSetDescRec       | Core              | Not                        ||
| SQLSetEnvAttr       | Core[2]           | Yes                        ||
| SQLSetPos           | Level 1[1]        | Yes                        ||
| SQLSetStmtAttr      | Core[2]           | Yes                        ||
| SQLSpecialColumns   | Core[1]           | Yes                        ||
| SQLStatistics       | Core              | Yes                        ||
| SQLTablePrivileges  | Level 2           | Yes                        ||
| SQLTables           | Core              | Yes                   	   ||


<a id="attriconfr"></a>
## Attribute Conformance of ODBC Supported APIs in Snappy Driver

### ODBC Environment Attribute

| Attributes                  | Conformance </br>Level | Supported in </br>Snappy Driver |Exceptions|
|-----------------------------|-------------------|----------------------------|---|
| SQL_ATTR_CONNECTION_POOLING | --[1]             | Yes                        |This is an optional feature and as such is not part of the conformance levels.|
| SQL_ATTR_CP_MATCH           | --[1]             | Yes                        ||
| SQL_ATTR_ODBC_VER           | Core              | Yes                        ||
| SQL_ATTR_OUTPUT_NTS         | --[1]             | Yes                        ||

### ODBC Connection Attribute

| Attributes                  | Conformance </br>Level  |  Supported in </br>Snappy Driver|Exceptions|
|-----------------------------|--------------------|----------------------------|---|
| SQL_ATTR_ACCESS_MODE        | Core               | Yes                        ||
| SQL_ATTR_ASYNC_ENABLE       | Level 1/Level 2[1] | Yes                        |Applications that support connection-level asynchrony (required for Level 1) must support setting this attribute to SQL_TRUE by calling SQLSetConnectAttr; the attribute need not be settable to a value other than its default value through SQLSetStmtAttr. Applications that support statement-level asynchrony (required for Level 2) must support setting this attribute to SQL_TRUE using either function.|
| SQL_ATTR_AUTO_IPD           | Level 2            | Not                        ||
| SQL_ATTR_AUTOCOMMIT         | Level 1            | Yes                        |For Level 1 interface conformance, the driver must support one value in addition to the driver-defined default value (available by calling SQLGetInfo with the SQL_DEFAULT_TXN_ISOLATION option). For Level 2 interface conformance, the driver must also support SQL_TXN_SERIALIZABLE.|
| SQL_ATTR_CONNECTION_DEAD    | Level 1            | Yes                        ||
| SQL_ATTR_CONNECTION_TIMEOUT | Level 2            | Yes                        ||
| SQL_ATTR_CURRENT_CATALOG    | Level 2            | Yes                        ||
| SQL_ATTR_LOGIN_TIMEOUT      | Level 2            | Yes                        ||
| SQL_ATTR_ODBC_CURSORS       | Core               | Yes                        ||
| SQL_ATTR_PACKET_SIZE        | Level 2            | Yes                        ||
| SQL_ATTR_QUIET_MODE         | Core               | Yes                        ||
| SQL_ATTR_TRACE              | Core               | Yes                        ||
| SQL_ATTR_TRACEFILE          | Core               | Yes                        ||
| SQL_ATTR_TRANSLATE_LIB      | Core               | Yes                        ||
| SQL_ATTR_TRANSLATE_OPTION   | Core               | Yes                        ||
| SQL_ATTR_TXN_ISOLATION      | Level 1/Level 2[2] | Yes                        ||

### ODBC Statement Attribute
| Attributes                     | Conformance </br>Level  |  Supported in </br>Snappy Driver |Exceptions|
|--------------------------------|--------------------|----------------------------|---|
| SQL_ATTR_APP_PARAM_DESC        | Core               | Yes                        ||
| SQL_ATTR_APP_ROW_DESC          | Core               | Yes                        ||
| SQL_ATTR_ASYNC_ENABLE          | Level 1/Level 2[1] | Yes                        ||
| SQL_ATTR_CONCURRENCY           | Level 1/Level 2[2] | Yes                        ||
| SQL_ATTR_CURSOR_SCROLLABLE     | Level 1            | Yes                        ||
| SQL_ATTR_CURSOR_SENSITIVITY    | Level 2            | Yes                        ||
| SQL_ATTR_CURSOR_TYPE           | Core/Level 2[3]    | Yes                        | Applications that support connection-level asynchrony (required for Level 1) must support setting this attribute to SQL_TRUE by calling SQLSetConnectAttr; the attribute need not be settable to a value other than its default value through SQLSetStmtAttr. Applications that support statement-level asynchrony (required for Level 2) must support setting this attribute to SQL_TRUE using either function.|
| SQL_ATTR_ENABLE_AUTO_IPD       | Level 2            | Yes                        ||
| SQL_ATTR_FETCH_BOOKMARK_PTR    | Level 2            | Yes                        | For Level 2 interface conformance, the driver must support SQL_CONCUR_READ_ONLY and at least one other value.|
| SQL_ATTR_IMP_PARAM_DESC        | Core               | Yes                        ||
| SQL_ATTR_IMP_ROW_DESC          | Core               | Yes                        |For Level 1 interface conformance, the driver must support SQL_CURSOR_FORWARD_ONLY and at least one other value. For Level 2 interface conformance, the driver must support all values defined in this document.|
| SQL_ATTR_KEYSET_SIZE           | Level 2            | Yes                        ||
| SQL_ATTR_MAX_LENGTH            | Level 1            | Yes                        ||
| SQL_ATTR_MAX_ROWS              | Level 1            | Yes                        ||
| SQL_ATTR_METADATA_ID           | Core               | Yes                        ||
| SQL_ATTR_NOSCAN                | Core               | Yes                        ||
| SQL_ATTR_PARAM_BIND_OFFSET_PTR | Core               | Yes                        ||
| SQL_ATTR_PARAM_BIND_TYPE       | Core               | Yes                        ||
| SQL_ATTR_PARAM_OPERATION_PTR   | Core               | Yes                        ||
| SQL_ATTR_PARAM_STATUS_PTR      | Core               | Yes                        ||
| SQL_ATTR_PARAMS_PROCESSED_PTR  | Core               | Yes                        ||
| SQL_ATTR_PARAMSET_SIZE         | Core               | Yes                        ||
| SQL_ATTR_QUERY_TIMEOUT         | Level 2            | Yes                        ||
| SQL_ATTR_RETRIEVE_DATA         | Level 1            | Yes                        ||
| SQL_ATTR_ROW_ARRAY_SIZE        | Core               | Yes                        ||
| SQL_ATTR_ROW_BIND_OFFSET_PTR   | Core               | Yes                        ||
| SQL_ATTR_ROW_BIND_TYPE         | Core               | Yes                        ||
| SQL_ATTR_ROW_NUMBER            | Level 1            | Yes                        ||
| SQL_ATTR_ROW_OPERATION_PTR     | Level 1            | Yes                        ||
| SQL_ATTR_ROW_STATUS_PTR        | Core               | Yes                        ||
| SQL_ATTR_ROWS_FETCHED_PTR      | Core               | Yes                        ||
| SQL_ATTR_SIMULATE_CURSOR       | Level 2            | Not                        ||
| SQL_ATTR_USE_BOOKMARKS         | Level 2            | Not                        ||