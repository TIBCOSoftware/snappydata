# Keywords and Identifiers

SQL statements are provided as strings to RowStore that must follow several rules.

-   Permitted character set is unicode
-   Single quotation marks delimit character strings
-   Within a character string, a single quotation mark itself can be represented using two single quotation marks
-   SQL keywords and identifiers are case-insensitive
-   Double quotation marks denote delimited identifiers as in the next section
-   In accordance with SQL-92 standard, comments can be single-line or multi-line. Single-line comments start with two dashes (--) and end with the newline character while multi-line comments start with forward slash star (/\*) and end with start forward slash (\*/).

##Standard SQL Identifiers

Two categories of identifiers are defined in SQL-92 standard: ordinary and delimited.

The rules mentioned here for SQL identifiers apply to all names used in SQL statements including table-name, column-name, view-name, synonym-name, constraint-name, correlation-name, index-name, trigger-name, function-name and procedure-name. There are two categories of identifiers as defined in SQL-92 standard: ordinary and delimited. The ordinary names must follow the below mentioned rules:

-   Can contain only unicode letters and digits, or underscore characters ( \_ )
-   Must begin with an alphabetic character
-   Should have a minimum length of one byte and a maximum of 128 bytes
-   Are case-insensitive (use delimited identifiers for case-sensitive)
-   Cannot contain quotation marks or spaces
-   Cannot be a reserved word

Delimited identifiers are those enclosed within double quotation (") marks. If the identifier itself needs to have a double quotation mark then two consecutive double quotation marks should be used.

The following conventions are used in the syntax definitions in the remaining document:

-   Square brackets (\[\]) are used to enclose optional items.
-   Braces ({}) are used to group items and is not part of syntax as such.
-   A star (\*) denotes that the preceding item can be repeated 0 or more number of times.
-   A pipe (|) is for ORing different elements i.e. one of the ORed items are needed.
-   An explicit bracket, brace, star or pipe will be shown enclosed in single quotes e.g. '\*' . Parentheses have no special meaning and are always literals.


