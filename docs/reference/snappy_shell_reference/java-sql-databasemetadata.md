# java.sql.DatabaseMetaData Interface

<a id="java-sql-databasemetadata__section_1B3CCBB2590346FAA2D4CF84BDA9A13D">The following sections describe `java.sql.DatabaseMetaData` functionality in SnappyData.</a>
-   <a href="java-sql-databasemetadata.html#java-sql-databasemetadata__section_C813D9A7C9044BB1A540B35CDA6F3517" class="xref">DatabaseMetaData Result Sets</a>
-   <a href="java-sql-databasemetadata.html#java-sql-databasemetadata__section_51DDFA8B7026432788B8F76FE9AC6D06" class="xref">java.sql.DatabaseMetaData.getProcedureColumns Method</a>
-   <a href="java-sql-databasemetadata.html#java-sql-databasemetadata__section_87386A01281B43469BC0012704BE1105" class="xref">Parameters to getProcedureColumns</a>
-   <a href="java-sql-databasemetadata.html#java-sql-databasemetadata__section_F9F0D4197E944136B1996DEC342CCD21" class="xref">java.sql.DatabaseMetaData.getBestRowIdentifier Method</a>

<a id="java-sql-databasemetadata__section_C813D9A7C9044BB1A540B35CDA6F3517"></a>

##DatabaseMetaData Result Sets

*DatabaseMetaData* result sets do not close the result sets of other statements, even when auto-commit is set to true.

*DatabaseMetaData* result sets are closed if a user performs any other action on a JDBC object that causes an automatic *commit* to occur. If you need the *DatabaseMetaData* result sets to be accessible while executing other actions that would cause automatic commits, turn off auto-commit with *setAutoCommit(false)*.

<a id="java-sql-databasemetadata__section_51DDFA8B7026432788B8F76FE9AC6D06"></a>

##java.sql.DatabaseMetaData.getProcedureColumns Method

SnappyData supports Java procedures and allows you to call Java procedures within SQL statements. SnappyData returns information about the parameters in the *getProcedureColumns* call and returns information for all Java procedures defined by CREATE PROCEDURE.

*getProcedureColumns* returns a ResultSet. Each row describes a single parameter or return value.

<a id="java-sql-databasemetadata__section_87386A01281B43469BC0012704BE1105"></a>

##Parameters to getProcedureColumns

The JDBC API defines the following parameters for the getProcedureColumns method call.

**catalog**   
Always use *null* for this parameter in SnappyData.

**schemaPattern**   
Java procedures have a schema.

**procedureNamePattern   **
A String object representing a procedure name pattern.

**column-Name-Pattern   **
A String object representing the name pattern of the parameter names or return value names. Java procedures have parameter names matching those defined in the CREATE PROCEDURE statement. Use "%" to find all parameter names.

<a id="java-sql-databasemetadata__section_C9699F70A52244F68111446F186AFE81"></a>

##Columns in the ResultSet Returned by getProcedureColumns

Columns in the *ResultSet* returned by *getProcedureColumns* are as described by the API. Further details for some specific columns:

**PROCEDURE\_CAT   **
Always "null" in SnappyData.

**PROCEDURE\_SCHEM   **
Schema for a Java procedure.

**PROCEDURE\_NAME **
Name of the procedure.

**COLUMN\_NAME   **
Name of the parameter. See column-Name-Pattern under <a href="java-sql-databasemetadata.html#java-sql-databasemetadata__section_87386A01281B43469BC0012704BE1105" class="xref">Parameters to getProcedureColumns</a>.

**COLUMN\_TYPE   **
Short indicating what the row describes. It is always *DatabaseMetaData.procedureColumnIn* for method parameters, unless the parameter is an array. If so, it is *DatabaseMetaData.procedureColumnInOut*. It always returns *DatabaseMetaData.procedureColumnReturn* for return values.

**TYPE\_NAME   **
SnappyData-specific name for the type.

**NULLABLE**   
Always returns *DatabaseMetaData.procedureNoNulls* for primitive parameters and *DatabaseMetaData.procedureNullable* for object parameters

**REMARKS**   
String describing the java type of the method parameter.

**COLUMN\_DEF   **
String describing the default value for the column (may be null).

**SQL\_DATA\_TYPE   **
Reserved by JDBC spec for future use.

**SQL\_DATETIME\_SUB   **
Reserved by JDBC spec for future use.

**CHAR\_OCTET\_LENGTH   **
Maximum length of binary and character based columns (or any other datatype the returned value is a NULL).

**ORDINAL\_POSITION   **
Ordinal position, starting from 1, for the input and output parameters for a procedure.

**IS\_NULLABLE   **
String describing the parameter's nullability (YES means parameter can include NULLs, NO means it cannot).

**SPECIFIC\_NAME   **
Name that uniquely identifies this procedure within its schema.

**METHOD\_ID   **
SnappyData-specific column.

**PARAMETER\_ID   **
SnappyData-specific column.

<a id="java-sql-databasemetadata__section_F9F0D4197E944136B1996DEC342CCD21"></a>

##java.sql.DatabaseMetaData.getBestRowIdentifier Method

The *java.sql.DatabaseMetaData.getBestRowIdentifier* method looks for identifiers in the following order:

-   A primary key on the table.

-   A unique constraint or unique index on the table.

-   All of the columns in the table.

This order might not return a unique row.

!!!Note
If the *java.sql.DatabaseMetaData.getBestRowIdentifier* method does not find a primary key, unique constraint, or unique index, the method must look for identifiers in all of the columns in the table. When the method looks for identifiers this way, the method will always find a set of columns that identify a row. However, a unique row might not be identified if there are duplicate rows in the table.
