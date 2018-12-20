# Data Types

The SQL type system determines the compile-time and runtime type of an expression. Each type has a certain range of permissible values that can be assigned to a column or value of that type.

The special value NULL, denotes an unassigned or missing value of any of the types (columns that have been assigned as non-nullable using NOT NULL clause or the primary key columns cannot have a NULL value). The supported types are given below.

- [ARRAY](#array)
- [BIGINT](#bigint)
- [BINARY](#binary)
- [BLOB](#blob)
- [BOOLEAN](#boolean)
- [BYTE](#byte)
- [CLOB](#clob)
- [CHAR](#char)
- [DATE](#date)
- [DECIMAL](#decimal)
- [DOUBLE](#double)
- [FLOAT](#float)
- [INT](#int)
- [INTEGER](#integer)
- [LONG](#long)
- [MAP](#map)
- [NUMERIC](#numeric)
- [REAL](#real)
- [SHORT](#short)
- [SMALLINT](#smallint)
- [STRING](#string)
- [STRUCT](#struct)
- [TIMESTAMP](#timestamp)
- [TINYINT](#tinyint)
- [VARBINARY](#varbinary)
- [VARCHAR](#varchar)

<!--
| Data Type | Description |
|--------|--------|
| [BIGINT](#bigint)|Provides 8-byte integer for long integer values|
| [BINARY](#binary)|Binary-encoded strings|
| [BLOB](#blob)|Carying-length binary string that can be up to 2,147,483,647 characters long|
| [BOOLEAN](#boolean)|Logical Boolean values (true/false)|
| [BYTE](#byte)|Binary data ("byte array")|
| [CLOB](#clob)|Text data in random-access chunks|
| [CHAR](#char)||
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
-->

<a id="array"></a>
## ARRAY

A column of ARRAY datatype can contain a collection of elements. 

**SQL Example**
```
# Create a table with column of type of an array of doubles and insert few records
CREATE TABLE IF NOT EXISTS Student(rollno Int, name String, marks Array<Double>) USING column;
INSERT INTO Student SELECT 1,'John', Array(97.8,85.2,63.9,45.2,75.2,96.5);
```

A column of type Array can store array of Java objects (Object[]), typed arrays, java.util.Collection and scala.collection.Seq. You can use **com.pivotal.gemfirexd.snappy.ComplexTypeSerializer** class to serialize the array data in order to insert it into column tables. Refer [How to store and retrieve complex data types in JDBC programs](/howto/store_retrieve_complex_datatypes_JDBC.md) for a Scala example that shows how to serialize and store an array in a table using JDBC APIs and **ComplexTypeSerializer** class.

!!! Note
	Supported only for column tables

<a id="bigint"></a>
## BIGINT

Provides 8 bytes storage for long integer values. An attempt to put a BIGINT value into another exact numeric type with smaller size/precision (e.g. INT) fails if the value overflows the maximum allowable by the smaller type.

For behavior with other types in expressions, see Numeric type promotion in expressions, Storing values of one numeric data type in columns of another numeric data type.

|                      |                                                   |
|----------------------|---------------------------------------------------|
| Equivalent Java type | java.lang.Long                                    |
| Minimum value        | java.lang.Long.MIN\_VALUE (-9223372036854775808 ) |
| Maximum value        | java.lang.Long.MAX\_VALUE (9223372036854775807 )  |

<a id="binary"></a>
## BINARY

<a id="blob"></a>
## BLOB

A binary large object represents an array of raw bytes of varying length.

|                                      |                                              |
|--------------------------------------|----------------------------------------------|
| Equivalent Java type                 | java.lang.Blob                               |
| Maximum length (also default length) | 2 GB - 1 (or 2,147,483,647)                  |

```pre
{ BLOB | BINARY LARGE OBJECT } [ ( length [{ K | M | G }] ) ] 
```

The length of the BLOB is expressed in number of bytes by default. The suffixes K, M, and G stand for kilobyte, megabyte and gigabyte, and use the multiples of 1024, 1024\*1024, or 1024\*1024\*1024 respectively.

```pre
CREATE TABLE blob_data(id INT primary key, data BLOB(10M)); 
–- search for a blob 
select length(data) from blob_data where id = 100;
```

<a id="boolean"></a>
## BOOLEAN

The data type representing `Boolean` values. This is equivalent to Java's `boolean` primitive type.

<a id="byte"></a>
## BYTE

The data type representing `Byte` values. It is an 8-bit signed integer (equivalent to Java's `byte` primitive type).

|                                      |                                              |
|--------------------------------------|----------------------------------------------|
| Minimum value                  | java.lang.Byte.MIN_VALUE                             |
| Maximum value | java.lang.Byte.MAX_VALUE |

<a id="char"></a>
## CHAR

Provides for fixed-length strings. If a string value is shorter than the expected length, then spaces are inserted to pad the string to the expected length. If a string value is longer than the expected length, then any trailing blanks are trimmed to make the length same as the expected length, while an exception is raised if characters other than spaces are required to be truncated. For comparision operations, the shorter CHAR string is padded with spaces to the longer value. Similarly when mixing CHARs and VARCHARs in expressions , the shorter value is padded with spaces to the length of longer string.

To represent a single quotation mark within a string, use two quotation marks:

```pre
VALUES 'going to Chandra''s place' 
```

The length of CHAR is an unsigned integer constant.

|                      |                                                  |
|----------------------|--------------------------------------------------|
| Equivalent Java type | java.lang.String                                 |
| Maximum length       | java.lang.Integer.MAX\_VALUE (2147483647 )       |
| Default length       | 1                                                |

```pre
CHAR[ACTER] [(length)] 
```

<a id="clob"></a>

## CLOB

A character large object represents an array of characters of varying length. It is used to store large character-based data such as documents.

The length is expressed in number characters, unless you specify the suffix K, M, or G, which uses the multiples of 1024, 1024\*1024, or 1024\*1024\*1024 respectively.

|                                      |                                              |
|--------------------------------------|----------------------------------------------|
| Equivalent Java type                 | java.sql.Clob                                |
| Maximum length (also default length) | 2 GB - 1 (or 2,147,483,647)                  |

```pre
{ CLOB | CHARACTER LARGE OBJECT } [ ( length [{ K | M | G }] ) ] 
```

```pre
CREATE TABLE clob_data(id INT primary key, text CLOB(10M)); 
–- search for a clob
select text from clob_data where id = 100;
```

<a id="date"></a>

## DATE

Provides for storage of a date as year-month-day. Supported formats are:

```pre
yyyy-mm-dd 
```

```pre
mm/dd/yyyy 
```

```pre
dd.mm.yyyy 
```

The year (yyyy) must always be expressed with four digits, while months (mm) and days (dd) may have either one or two digits. DATEs, TIMEs, and TIMESTAMPs must not be mixed with one another in expressions except with an explicit CAST.

|                      |                                              |
|----------------------|----------------------------------------------|
| Equivalent Java type | java.sql.Date                                |

```pre
VALUES '2010-05-04'
```

```pre
VALUES DATE('2001-10-12')
```

The latter example uses the DATE() function described in the section Built-in functions and procedures.

<a id="decimal"></a>

## DECIMAL

Provides an exact decimal value having a specified precision and scale. The precision is the total number of digits both to the left and the right of the decimal point, and the scale is the number of digits in the fraction to the right of the decimal point.

A numeric value (e.g. INT, BIGINT, SMALLINT) can be put into a DECIMAL as long as non-fractional precision is not lost else a range exception is thrown (SQLState: "22003"). When truncating trailing digits from a DECIMAL, the value is rounded down.

For behavior with other types in expressions, see Numeric type promotion in expressions, Scale for decimal arithmetic and Storing values of one numeric data type in columns of another numeric data type.

|                      |                                                          |
|----------------------|----------------------------------------------------------|
| Equivalent Java type | java.math.BigDecimal                                     |
| Precision min/max    | 1 to 31                                                  |
| Scale min/max        | less than or equal to precision                          |
| Default precision    | 5                                                        |
| Default scale        | 0                                                        |

```pre
{ DECIMAL | DEC } [(precision [, scale ])]
```

```pre
-- this cast loses fractional precision 
values cast (23.8372 AS decimal(4,1)); 
-–- results in: 
23.8 
-- this cast is outside the range 
values cast (97824 AS decimal(4,1)); 
–-- throws exception: 
ERROR 22003: The resulting value is outside the range for the data type DECIMAL/NUMERIC(4,1). 
```

<a id="double"></a>

## DOUBLE

Provides 8-byte storage for numbers using IEEE floating-point notation.

Arithmetic operations do not round their resulting values to zero. If the values are too small, you will receive an exception. Numeric floating point constants are limited to a length of 30 characters.

For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

|                         |                                                                                                                                                                  |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Equivalent Java type    | java.lang.Double <p class="note"><strong>Note:</strong> The maximum/minimum limits are different from those of java.lang.Double as noted below. </p> |
| Minimum value           | -1.79769E+30                                                                                                                                                     |
| Maximum value           | 1.79769E+308                                                                                                                                                     |
| Smallest positive value | 2.225E-307                                                                                                                                                       |
| Largest negative value  | -2.225E-307                                                                                                                                                      |
| Default precision       | 5                                                                                                                                                                |
| Default scale           | 0                                                                                                                                                                |

```pre
–- examples of valid values 
values 233.31E3; 
values 8928E+06; 
-- this example will throw a range exception (SQLState: "42820") 
values 123456789012345678901234567890123456789e0; 
```

<a id="float"></a>

## FLOAT

Alias for a REAL or DOUBLE data type, depending on the specified precision. The default precision is 53 making it equivalent to DOUBLE. A precision of 23 or less makes FLOAT equivalent to REAL while greater than 23 makes it equivalent to DOUBLE.

|                        |                                                                                                                       |
|------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Equivalent Java type   | java.lang.Double or java.lang.Float depending on precision                                                            |
| Minumum/Maximum limits | Same as those for FLOAT if the precision is less than 23. Otherwise, same minimum/maximum limits as those for DOUBLE. |
| Default precision      | 53                                                                                                                    |

```pre
FLOAT [(precision)]
```

<a id="int"></a>

## INT

<a id="integer"></a>

## INTEGER

Provides 4 bytes storage for integer values. INT can be used as a synonym for INTEGER in CREATE TABLE.

For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

|                      |                                            |
|----------------------|--------------------------------------------|
| Equivalent Java type | java.lang.Integer                          |
| Minimum value        | java.lang.Integer.MIN\_VALUE (-2147483648) |
| Maximum value        | java.lang.Integer.MAX\_VALUE (21474836487) |


<a id="long"></a>
## LONG


The data type representing `Long` values. It's a 64-bit signed integer (equivalent to Java's `long` primitive type).

|                      |                                            |
|----------------------|--------------------------------------------|
| Minimum value   | java.lang.Long.MIN_VALUE                          |
|    Maximum value   | java.lang.Long.MAX_VALUE |

!!! Note
	Supported only for column tables


<a id="numeric"></a>

<a id="map"></a>
## MAP


A column of MAP datatype can contain a collection of key-value pairs. 

**SQL Examples**

```
# Create a table with column of type MAP and insert few records
CREATE TABLE IF NOT EXISTS StudentGrades (rollno Integer, name String, Course Map<String, String>) USING column;
INSERT INTO StudentGrades SELECT 1,'Jim', Map('English', 'A+');
INSERT INTO StudentGrades SELECT 2,'John', Map('English', 'A', 'Science', 'B');
```
```
# Selecting grades for 'English'

snappy> select ROLLNO, NAME, course['English'] from StudentGrades;

ROLLNO  |NAME  |COURSE[English]     
---------------------------
2       |John  |A                                                                                          
1       |Jim   |A+         
```                                                                                


A column of type Map can store **java.util.Map** or **scala.collection.Map**. You can use **com.pivotal.gemfirexd.snappy.ComplexTypeSerializer** class to serialize the map data in order to insert it into column tables. Refer [How to store and retrieve complex data types in JDBC programs](/howto/store_retrieve_complex_datatypes_JDBC.md) for a Scala example that shows how to serialize and store an array in a table using JDBC APIs and **ComplexTypeSerializer** class. Map data can also be stored in a similar way.

!!! Note
	Supported only for column tables

## NUMERIC

Synonym for DECIMAL data type.

The meta-data differences from DECIMAL are listed below. Otherwise, NUMERIC behaves identically to DECIMAL.

|                    |                        |
|--------------------|------------------------|
|  |  |

```pre
NUMERIC [(precision [, scale ])]
```
<a id="real"></a>

## REAL

<a id="short"></a>

## SHORT

<a id="smallint"></a>

## SMALLINT

Provides 2 bytes storage for short integer values.

For behavior with other types in expressions, see Numeric type promotion in expressions, and Storing values of one numeric data type in columns of another numeric data type.

|                      |                                                |
|----------------------|------------------------------------------------|
| Equivalent Java type | java.lang.Short                                |
| Minimum value        | java.lang.Short.MIN\_VALUE (-32768 )           |
| Maximum value        | java.lang.Short.MAX\_VALUE (32767)             |


<a id="string"></a>
## STRING

The data type representing `String` values. A String encoded in UTF-8 as an Array[Byte], which can be used for comparison search.

<a id="struct"></a>
## STRUCT
A column of struct datatype can contain a structure with different fields. 

**SQL Examples**
```
# Create a table with column of type STRUCT and insert few records.

CREATE TABLE IF NOT EXISTS StocksInfo (SYMBOL STRING, INFO STRUCT<TRADING_YEAR: STRING, AVG_DAILY_VOLUME: LONG, HIGHEST_PRICE_IN_YEAR: INT, LOWEST_PRICE_IN_YEAR: INT>) USING COLUMN;
INSERT INTO StocksInfo SELECT 'ORD', STRUCT('2018', '400000', '112', '52');
INSERT INTO StocksInfo SELECT 'MSGU', Struct('2018', '500000', '128', '110');
```

```
# Select symbols with average daily volume is more than 400000

SELECT SYMBOL FROM StocksInfo WHERE INFO.AVG_DAILY_VOLUME > 400000;
SYMBOL
-------------------------------------------------------------------------
MSGU       

```
A column of type STRUCT can store array of Java objects (Object[]), typed arrays, java.util.Collection, scala.collection.Seq or scala.Product. You can use **com.pivotal.gemfirexd.snappy.ComplexTypeSerializer** 
class to serialize the data in order to insert it into column tables. Refer [How to store and retrieve complex data types in JDBC programs](/howto/store_retrieve_complex_datatypes_JDBC.md) for a Scala example that shows how to serialize and store an array in a table using JDBC APIs and **ComplexTypeSerializer** class.

<a id="timestamp"></a>
## TIMESTAMP

Provides for storage of both DATE and TIME as a combined value. In addition it allows for fractional seconds having up to six digits. Supported formats are:

```pre
yyyy-MM-dd hh:mm:ss[.nnnnnn] 
```

```pre
yyyy-MM-dd-hh.mm.ss[.nnnnnn] 
```

The year (yyyy) must always be expressed with four digits. Months (MM), days (dd), and hours (hh) may have one or two digits while minutes (mm) and seconds (ss) must have two digits. Microseconds, if present, may have between one and six digits. DATEs, TIMEs, and TIMESTAMPs must not be mixed with one another in expressions except with an explicit CAST.

|                      |                                                        |
|----------------------|--------------------------------------------------------|
| Equivalent Java type | java.sql.Timestamp                                     |

```pre
VALUES '2000-02-03 12:23:04' 
VALUES TIMESTAMP(' 2000-02-03 12:23:04.827') 
VALUES TIMESTAMP('2000-02-03 12:23:04')
```

The latter examples use the TIMESTAMP() function described in the section Built-in functions and procedures.

<a id="tinyint"></a>
## TINYINT


<a id="varbinary"></a>
## VARBINARY

<a id="varchar"></a>
## VARCHAR

Provides for variable-length strings with a maximum limit for length. If a string value is longer than the maximum length, then any trailing blanks are trimmed to make the length same as the maximum length, while an exception is raised if characters other than spaces are required to be truncated. When mixing CHARs and VARCHARs in expressions, the shorter value is padded with spaces to the length of longer string.

The type of a string constant is CHAR, not VARCHAR. To represent a single quotation mark within a string, use two quotation marks:

```pre
VALUES 'going to Chandra''s place' 
```

The length of VARCHAR is an unsigned integer constant.

|                      |                                                  |
|----------------------|--------------------------------------------------|
| Equivalent Java type | java.lang.String                                 |
| Maximum length       | 32672                                            |

```pre
{ VARCHAR | CHAR VARYING | CHARACTER VARYING }(length)
```

