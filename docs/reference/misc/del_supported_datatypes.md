# Data Types
The SQL type system determines the compile-time and runtime type of an expression. Each type has a certain range of permissible values that can be assigned to a column or value of that type.

The special value NULL, denotes an unassigned or missing value of any of the types (columns that have been assigned as non-nullable using NOT NULL clause or the primary key columns cannot have a NULL value). The supported types are given below:

**Data Types Supported for Column and Row Tables**

| Data Type | Description |
|--------|--------|
| [BIGINT](#bigint)|Provides 8-byte integer for long integer values|
| [BINARY](#binary)|Binary-encoded strings|
| [BLOB](#blob)|Carying-length binary string that can be up to 2,147,483,647 characters long|
| [BOOLEAN](#boolean)|Logical Boolean values (true/false)|
| [BYTE](#byte)|Binary data ("byte array")|
| [CLOB](#clob)|Text data in random-access chunks|
| [DATE](#date)|Calendar dates (year, month, day)|
| [DECIMAL](#decimal)|DECIMAL(p) floating point and DECIMAL(p,s) fixed point|
| [DOUBLE](#double)|A double-precision floating point value|
| [FLOAT](#float)|Stores floating point value: that is a number with a fractional part|
| [INT](#int)|Stores integer: a whole number|
| [INTEGER](#integer)|Stores signed four-byte integer|
| [LONG](#long)|Stores character strings with a maximum length of 32,700 characters|
| [NUMERIC](#numeric)|Stores exact numeric of selectable precision|
| [REAL](#real)|Stores single precision floating-point number (4 bytes)|
| [SHORT](#short)|The size	 of the short type is 2 bytes (16 bits) |
| [SMALLINT](#smallint)|Stores signed two-byte integer|
| [STRING](#string)|Stores are sequences of characters|
| [TIMESTAMP](#timestamp)|Stores date and time as a combined value|
| [TINYINT](#tinyint)|Stores a very small integer. The signed range is -128 to 127.|
| [VARBINARY](#varbinary)|Stores binary byte strings rather than non-binary character strings|
| [VARCHAR](#varchar)|Stores character strings of varying length (up to 255 bytes); collation is in code-set order|

<a href="bigint"></a>
## BIGINT

Provides 8 bytes storage for long integer values. An attempt to put a BIGINT value into another exact numeric type with smaller size/precision (e.g. INT) fails if the value overflows the maximum allowable by the smaller type.

For behavior with other types in expressions, see [Numeric type](#numeric) promotion in expressions, Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType BIGINT`

<a href="binary"></a>
## BINARY
**Description** </br>
The BINARY type is similar to the CHAR type, but stores binary byte strings rather than non-binary character strings. </br>
It contains no character set, and comparison and sorting are based on the numeric value of the bytes.

**Field Details** </br>
`public static final DataType BinaryType`

<a href="blob"></a>
## BLOB
**Description** </br>
A binary large object represents an array of raw bytes of varying length.

**Field Details** </br>
`public static final DataType BLOB`

<a href="boolean"></a>
## BOOLEAN
**Description** </br>
Booleans are used to represent true and false values returned by comparison operators and logical functions. The values are, true and false. </br>
The display values can be localized.

**Field Details** </br>
`public static final DataType BOOLEAN`

<a href="byte"></a>
## BYTE
**Description** </br>
The BYTE data type stores any kind of binary data in an undifferentiated byte stream. Binary data typically consists of digitized information, such as spreadsheets, program load modules, digitized voice patterns, and so on.</br>
The term simple large object refers to an instance of a TEXT or BYTE data type. No more than 195 columns of the same table can be declared as BYTE and TEXT data types.

**Field Details** </br>
`public static final DataType ByteType`

<a href="clob"></a>
## CLOB
**Description** </br>
A character large object represents an array of characters of varying length. It is used to store large character-based data such as documents.</br>
The length is expressed in number characters, unless you specify the suffix K, M, or G, which uses the multiples of 1024, 1024*1024, or 1024*1024*1024 respectively.

**Field Details** </br>
`public static final DataType CLOB`

<a href="date"></a>
## DATE
**Description** </br>
Provides for storage of a date as year-month-day. Supported formats are: `yyyy-mm-dd`, `mm/dd/yyyy`, and `dd.mm.yyyy`</br>
The year (yyyy) must always be expressed with four digits, while months (mm) and days (dd) may have either one or two digits. DATEs, TIMEs, and TIMESTAMPs must not be mixed with one another in expressions except with an explicit CAST.

**Field Details** </br>
`public static final DataType DATE`
Gets the DateType object.

<a href="decimal"></a>
## DECIMAL
**Description** </br>
Provides an exact decimal value having a specified precision and scale. The precision is the total number of digits both to the left and the right of the decimal point, and the scale is the number of digits in the fraction to the right of the decimal point.</br>
A numeric value (e.g. INT, BIGINT, SMALLINT) can be put into a DECIMAL as long as non-fractional precision is not lost else a range exception is thrown (SQLState: "22003"). When truncating trailing digits from a DECIMAL, the value is rounded down.</br>
For behavior with other types in expressions, see Numeric type promotion in expressions, Scale for decimal arithmetic and Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType DECIMAL`

<a href="double"></a>
## DOUBLE
**Description** </br>
Provides 8-byte storage for numbers using IEEE floating-point notation.</br> 
Arithmetic operations do not round their resulting values to zero. If the values are too small, you will receive an exception. Numeric floating point constants are limited to a length of 30 characters.</br>
For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType DOUBLE`
Gets the DoubleType object.

<a href="float"></a>
## FLOAT
**Description** </br>
Alias for a REAL or DOUBLE data type, depending on the specified precision. The default precision is 53 making it equivalent to DOUBLE. A precision of 23 or less makes FLOAT equivalent to REAL while greater than 23 makes it equivalent to DOUBLE.

**Field Details** </br>
`public static final DataType FLOAT`

<a href="int"></a>
## INT
**Description** </br>
The INT data type is a synonym for INTEGER.
**Field Details** </br>
`public static final DataType INT`

<a href="integer"></a>
## INTEGER
**Description** </br>
Integer values are written as a sequence of digits. It provides 4 bytes storage for integer values. INT can be used as a synonym for INTEGER in CREATE TABLE.</br>
For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType IntegerType`

<a href="long"></a>
## LONG
**Description** </br>
 The long data type is a 64-bit two's complement integer. The signed long has a minimum value of -263 and a maximum value of 263-1. Use this data type when you need a range of values wider than those provided by int. 
**Field Details** </br>
`public static final DataType LongType`

<a href="numeric"></a>
## NUMERIC
**Description** </br>
Synonym for DECIMAL data type.</br>
The meta-data differences from DECIMAL are listed below. Otherwise, NUMERIC behaves identically to DECIMAL.

**Field Details** </br>
`public static final DataType NUMERIC`

<a href="real"></a>
## REAL
**Description** </br>
Provides 4-byte storage for numbers using IEEE floating-point notation.</br>
Arithmetic operations do not round their resulting values to zero. If the values are too small, you will receive an exception. Constants always map to DOUBLE – use an explicit CAST to convert a constant to REAL.</br>
For behavior with other types in expressions, see Numeric type promotion in expressions, Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType REAL`

<a href="short"></a>
## SHORT
**Description** </br>
The short data type is a 16-bit signed two's complement integer. It has a minimum value of -32,768 and a maximum value of 32,767 (inclusive). As with byte, the same guidelines apply: you can use a short to save memory in large arrays, in situations where the memory savings actually matters.

**Field Details** </br>
`public static final DataType ShortType`

<a href="smallint"></a>
## SMALLINT
**Description** </br>
Provides 2 bytes storage for short integer values.</br>
For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

**Field Details** </br>
`public static final DataType SMALLINT`

<a href="string"></a>
## STRING
The string type is used for storing text strings. A text string is a sequence of characters in the Unicode format with the final zero at the end of it. A string constant can be assigned to a string variable. A string constant is a sequence of Unicode characters enclosed in quotes or double quotes: "This is a string constant".</br>
To include a double quote (") into a string, the backslash character (\) must be put before it. The \ (backslash character) is used to escape characters. Any special character constants can be written in a string, if the backslash character (\) is typed before them.

**Field Details** </br>
`public static final DataType StringType`
Gets the StringType object.

<a href="timestamp"></a>
## TIMESTAMP
**Description** </br>
Provides for storage of both DATE and TIME as a combined value. In addition it allows for fractional seconds having up to six digits. Supported formats are:</br>

- yyyy-MM-dd hh:mm:ss[.nnnnnn] 
- yyyy-MM-dd-hh.mm.ss[.nnnnnn] 

The year (yyyy) must always be expressed with four digits. Months (MM), days (dd), and hours (hh) may have one or two digits while minutes (mm) and seconds (ss) must have two digits. Microseconds, if present, may have between one and six digits. DATEs, TIMEs, and TIMESTAMPs must not be mixed with one another in expressions except with an explicit CAST.

**Field Details** </br>
`public static final DataType TimestampType`

<a href="tinyint"></a>
## TINYINT
**Description** </br>
A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255. 

**Field Details** </br>
`public static final TINYINT`

<a href="varbinary"></a>
## VARBINARY
**Description** </br>
The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than non-binary character strings.</br>
It contains no character set, and comparison and sorting are based on the numeric value of the bytes.</br>

**Field Details** </br>
`public static final DataType VARBINARY`

<a href=""></a>
## VARCHAR
**Description** </br>
Provides for variable-length strings with a maximum limit for length. If a string value is longer than the maximum length, then any trailing blanks are trimmed to make the length same as the maximum length, while an exception is raised if characters other than spaces are required to be truncated. When mixing CHARs and VARCHARs in expressions, the shorter value is padded with spaces to the length of longer string.</br>
The type of a string constant is CHAR, not VARCHAR. To represent a single quotation mark within a string, use two quotation marks:

`VALUES 'visiting John's place' `

The length of VARCHAR is an unsigned integer constant.

**Field Details** </br>
`public static final DataType VARCHAR`