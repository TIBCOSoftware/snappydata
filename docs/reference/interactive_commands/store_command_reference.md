# Snappy-SQL Shell Interactive Commands

`snappy` implements an interactive command-line tool that is based on the Apache Derby `ij` tool. Use `snappy` to run scripts or interactive queries against a SnappyData cluster.

Start the interactive `snappy` command prompt by using the snappy script without supplying any other options:

```pre
snappy
```

The system property `snappy.history` specifies a file in which to store all of the commands executed during an interactive `snappy` session. For example:

```pre
$ export JAVA_ARGS="-Dsnappy.history=/Users/user1/snappydata-history.sql"
$ snappy
```

By default the history file is named .snappy.history, and it is stored in the current user's home directory.

`snappy` accepts several commands to control its use of JDBC. It recognizes a semicolon as the end of a `snappy` or SQL command. It treats semicolons within SQL comments, strings, and delimited identifiers as part of those constructs and not as the end of the command. Semicolons are required at the end of a `snappy` or SQL statement.

All `snappy` commands, identifiers, and keywords are case-insensitive.

Commands can span multiple lines without using any special escape character for ends of lines. This means that if a string spans a line, the new line contents show up in the value in the string.

`snappy` treats any command that it does not recognize as a SQL command that is passed to the underlying connection. This means that any syntactic errors in `snappy` commands are handed to the SQL engine and generally result in SQL parsing errors.

-	**[autocommit](../../reference/interactive_commands/autocommit.md)**
	Turns the connection's auto-commit mode on or off.

-	**[commit](../../reference/interactive_commands/commit.md)**
	Issues a `java.sql.Connection.commit` request.

-   **[connect client](../../reference/interactive_commands/connect_client.md)**
    Using the JDBC SnappyData thin client driver, connects to a SnappyData member indicated by the *host:port* values.
    
-   **[connect](../../reference/interactive_commands/connect.md)**
    Using the JDBC SnappyData thin client driver, connects to a SnappyData member indicated by the *host:port* values.

-   **[describe](../../reference/interactive_commands/describe.md)**
    Provides a description of the specified table or view.

-   **[disconnect](../../reference/interactive_commands/disconnect.md)**
    Disconnects from the database.

-   **[elapsedtime](../../reference/interactive_commands/elapsedtime.md)**
    Displays the total time elapsed during statement execution.

-   **[exit](../../reference/interactive_commands/exit.md)**
    Completes the `snappy` application and halts processing.

-   **[MaximumDisplayWidth](../../reference/interactive_commands/maximumdisplaywidth.md)**
    Sets the largest display width for columns to the specified value.

-   **[rollback](../../reference/interactive_commands/rollback.md)**
	Issues a `java.sql.Connection.rollback` request.

-   **[run](../../reference/interactive_commands/run.md)**
    Treats the value of the string as a valid file name, and redirects `snappy` processing to read from that file until it ends or an exit command is executed.

-   **[set connection](../../reference/interactive_commands/set_connection.md)**
    Specifies which connection to make current when more than one connection is open.

-   **[show](../../reference/interactive_commands/show.md)**
    Displays information about active connections and database objects.
