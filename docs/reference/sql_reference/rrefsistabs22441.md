# SYSCOLUMNS

Describes the columns within all tables in the RowStore distributed system.

<a id="rrefsistabs22441__section_F2D50CAD34484E52A1F77E0339DEC051"></a><a id="rrefsistabs22441__table_3C4F29A966624276988076D6AAFC8DC5"></a>

|Column Name|Type|Length|Nullable|Contents|
|- |- |- |-||-|
|REFERENCEID|CHAR|36|No|Identifier for table (join with SYSTABLES.TABLEID)|
|COLUMNNAME|VARCHAR|128|No|Column or parameter name|
|COLUMNNUMBER|INT|10|No|The position of the column within the table|
|COLUMNDATATYPE|'com.pivotal. snappydata.internal. catalog.TypeDescriptor'</br>This class is not part of the public API.|2,147,483,647|No|System type that describes precision, length, scale, nullability, type name, and storage type of data.</br> For a user-defined type, this column can hold a 'TypeDescriptor' that refers to the appropriate type alias in SYS.SYSALIASES.|
|COLUMNDEFAULT|'java.io.Serializable'|2,147,483,647|Yes|For tables, describes default value of the column</br> The 'toString()' method on the object stored in the table returns the text of the default value as specified in the CREATE TABLE or ALTER TABLE statement.|
|COLUMNDEFAULTID|CHAR|36|Yes|Unique identifier for the default value|
|AUTOINCREMENTVALUE|BIGINT|19|Yes|What the next value for column will be, if the column is an identity column|
|AUTOINCREMENTSTART|BIGINT|19|Yes|Initial value of column (if specified), if it is an identity column|
|AUTOINCREMENTINC|BIGINT|19|Yes|Amount column value is automatically incremented (if specified), if the column is an identity column|

