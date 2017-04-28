# java.sql.PreparedStatement Interface

SnappyData provides all required JDBC type conversions and additionally allows use of the individual `setXXX` methods for each type as if a `setObject(Value, JDBCTypeCode)` invocation were made. This means that `setString` can be used for any built-in target type. Cursors are not supported in SnappyData; `setCursorName` method throws an unsupported feature exception.

##Prepared Statements and Streaming Columns

`setXXXStream` requests stream data between the client and the server. JDBC allows an IN parameter to be set to a Java input stream for passing in large amounts of data in smaller chunks. When the statement is run, the JDBC driver makes repeated calls to this input stream. SnappyData supports the three types of streams that the JDBC API provides:

-   `setBinaryStream` Use for streams that contain uninterpreted bytes.

-   `setAsciiStream` Use for streams that contain ASCII characters.

-   `setUnicodeStream` Use for streams that contain Unicode characters.

The stream object passed to these three methods can be either a standard Java stream object or the user's own subclass that implements the standard `java.io.InputStream` interface. According to the JDBC standard, streams can be stored only in columns with the data types shown in the following table.

<a id="java-sql-preparedstatement__section_53ECFCBDA6E5481BA3C4075E80BA8F68"></a>

##Streamable JDBC Data Types

|                  |                         |             |               |              |
|------------------|-------------------------|-------------|---------------|--------------|
| Column Data Type | Corresponding Java Type | AsciiStream | UnicodeStream | BinaryStream |
| CLOB             | java.sql.Clob           | x           | x             | '            |
| CHAR             | '                       | x           | x             | '            |
| VARCHAR          | '                       | x           | x             | '            |
| LONGVARCHAR      | '                       | **X**       | **X**         | '            |
| BINARY           | '                       | x           | x             | x            |
| BLOB             | java.sql.Blob           | x           | x             | x            |
| VARBINARY        | '                       | x           | x             | x            |
| LONGVARBINARY    | '                       | x           | x             | **X**        |

!!! Note:
-   A large X indicates the preferred target data type for the type of stream. See <a href="mapping-types.html#mappingtypes" class="xref">Mapping java.sql.Types to SQL Types</a>.

-   If the stream is stored in a column of a type other than LONG VARCHAR or LONG VARCHAR FOR BIT DATA, the entire stream must be able to fit into memory at one time. Streams stored in LONG VARCHAR and LONG VARCHAR FOR BIT DATA columns do not have this limitation.

-   Streams cannot be stored in columns of the other built-in data types or columns of user-defined data types.

##Example

The following example shows how a user can store a streamed `java.io.File` in a LONG VARCHAR column:

``` pre
Statement s = conn.createStatement();
s.executeUpdate("CREATE TABLE atable (a INT, b LONG VARCHAR)");
conn.commit();
java.io.File file = new java.io.File("derby.txt");
int fileLength = (int) file.length();
// first, create an input stream
java.io.InputStream fin = new java.io.FileInputStream(file);
PreparedStatement ps = conn.prepareStatement("INSERT INTO atable VALUES (?, ?)"); 
ps.setInt(1, 1);
// set the value of the input parameter to the input stream
ps.setAsciiStream(2, fin, fileLength);
ps.execute();
conn.commit();
```
