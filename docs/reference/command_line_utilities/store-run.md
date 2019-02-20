# run
Connects to a SnappyData distributed system and executes the contents of a SQL command file. All commands in the specified file must be compatible with the interactive snappy SQL shell.

## Syntax

```pre
./bin/snappy run -file=<path or URL>
     [-auth-provider=<name>]
     [-client-bind-address=<address>]
     [-client-port=<port>]
     [-encoding=<charset>]
     [-extra-conn-props=<properties>] 
     [-help] 
     [-ignore-errors]
     [-J-D<property=value>]
     [-password[=<password>]]
     [-path=<path>]
     [-user=<username>]
```

This table describes options for the `snappy run` command. Default values are used if you do not specify an option.

|Option|Description|
|-|-|
|-file|The local path of a SQL command file to execute, or a URL that links to the SQL command file. All commands in the specified file must be compatible with the interactive snappy SQL shell.</br>This argument is required.|
|-auth-provider|Sets the authentication provider to use for peer-to-peer connections as well as client-server connections. Valid values are BUILTIN and LDAP. All other members of the SnappyData distributed system must use the same authentication provider and user definitions. If you omit this option, the connection uses no authentication mechanism.|
|-client-bind-address|Set the hostname or IP address to which the locator or server listens on for JDBC/ODBC/thrift client connections.|
|-client-port|The port on which a SnappyData locator listens for client connections. The default is 1527.</br>Use this option with `-client-bind-address` to attach to a SnappyData cluster as a thin client and perform the command.|
|-encoding|The character set encoding of the SQL script file (`-file` argument). The default is UTF-8. Other possible values are: US-ASCII, ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, UTF-16. See the [java.nio.charset.Charset](http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html) reference for more information.|
|-extra-conn-props|A semicolon-separated list of properties to use when connecting to the SnappyData distributed system.|
|help, --help|Display the help message for this snappy command.|
|-ignore-errors|Include this option to ignore any errors that may occur while executing statements in the file, and continue executing the remaining statements. If you omit this option, then snappy immediately terminates the script's execution if an exception occurs.|
|-J-D;property=value;|Sets Java system property to the specified value.|
|-password|If the servers or locators have been configured to use authentication, this option specifies the password for the user (specified with the -user option) to use for booting the server and joining the distributed system.</br>The password value is optional. If you omit the password, you are prompted to enter a password from the console.|
|-path|Configures the working directory for any other SQL command files executed from within the script. The `-path` entry is prepended to any SQL script file name executed that the script executes in a [run](../../reference/interactive_commands/store_command_reference.md) command.|
|-user|If the servers or locators have been configured to use authentication, this option specifies the username to use for booting the server and joining the distributed system.|

## Description

Specify the below command to connect to a SnappyData Distributed system and execute a SQL command file:

Use both `-client-bind-address` and `-client-port` to connect to a SnappyData cluster as a thin client and perform the command.

The `-file` argument specifies the location of the SQL script file to execute. If the script file itself calls other script files using `run 'filename'`, also consider using the `-path` option to specify the location of the embedded script files. If an exception occurs while executing the script, SnappyData immediately stops executing script commands, unless you include the `-ignore-errors` option.

## Examples

This command connects to a SnappyData network server running on localhost:1527 and executes commands in the create_and_load_column_table.sql file:

```pre
./bin/snappy run -file=/home/user1/snappydata/examples/quickstart/scripts/create_and_load_column_table.sql -client-bind-address=localhost -client-port=1527 
```

If the script calls for dependent scripts (for example load_countries.sql, load_cities.sql) and if the command is executed outside the directory in which the dependent scripts are located, specify the working directory using the `-path` option.

```pre
./bin/snappy run -file=/home/user1/snappydata/examples/quickstart/scripts/create_and_load_column_table.sql -path=/home/user1/snappydata/examples/quickstart -client-bind-address=localhost -client-port=1527
```
You can also run the command by providing the username and password.

```
./bin/snappy run -file=/home/supriya/snappy/snappydata/examples/quickstart/scripts/create_and_load_column_table.sql -client-bind-address=localhost -client-port=1527 -user=user1 -password=user123

```
