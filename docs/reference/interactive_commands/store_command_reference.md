#snappy-shell Interactive Commands

`snappy-shell` implements an interactive command-line tool that is based on the Apache Derby `ij` tool. Use `snappy-shell` to run scripts or interactive queries against a SnappyData cluster.

Start the interactive `snappy-shell` command prompt by using the <span class="ph filepath">snappy-shell</span> script without supplying any other options:

``` pre
snappy-shell
```

The system property `snappydata.history` specifies a file in which to store all of the commands executed during an interactive `snappy-shell` session. For example:

``` pre
$ export JAVA_ARGS="-Dsnappydata.history=/Users/yozie/snappydata-history.sql"
$ snappy
```

By default the history file is named <span class="ph filepath">.snappydata.history</span>, and it is stored in the current user's home directory.

`snappy-shell` accepts several commands to control its use of JDBC. It recognizes a semicolon as the end of a `snappy-shell` or SQL command. It treats semicolons within SQL comments, strings, and delimited identifiers as part of those constructs and not as the end of the command. Semicolons are required at the end of a `snappy-shell` or SQL statement.

All `snappy-shell` commands, identifiers, and keywords are case-insensitive.

Commands can span multiple lines without using any special escape character for ends of lines. This means that if a string spans a line, the new line contents show up in the value in the string.

`snappy-shell` treats any command that it does not recognize as a SQL command that is passed to the underlying connection. This means that any syntactic errors in `snappy-shell` commands are handed to the SQL engine and generally result in SQL parsing errors.

-   **[absolute](../../reference/store_commands/absolute.html)**
    Moves the cursor to the row specified by the *int*, and then fetches the row.
-   **[after last](../../reference/store_commands/after_last.html)**
    Moves the cursor to after the last row, then fetches the row.
-   **[async](../../reference/store_commands/async.html)**
    Execute an SQL statement in a separate thread.
-   **[autocommit](../../reference/store_commands/autocommit.html)**
    Turns the connection's auto-commit mode on or off.
-   **[before first](../../reference/store_commands/before_first.html)**
    Moves the cursor before the first row, then fetches the row.
-   **[close](../../reference/store_commands/close.html)**
    Closes the named cursor.
-   **[commit](../../reference/store_commands/commit.html)**
    Issues a *java.sql.Connection.commit* request.
-   **[connect](../../reference/store_commands/connect.html)**
    Connects to the database indicated by the *ConnectionURLString*.
-   **[connect client](../../reference/store_commands/connect_client.html)**
    Using the JDBC SnappyData thin client driver, connects to a SnappyData member indicated by the *host:port* values.
-   **[connect peer](../../reference/store_commands/connect_peer.html)**
    Using the JDBC SnappyData peer client driver, connects to a SnappyData member with specified boot and connection property values.
-   **[describe](../../reference/store_commands/describe.html)**
    Provides a description of the specified table or view.
-   **[disconnect](../../reference/store_commands/disconnect.html)**
    Disconnects from the database.
-   **[driver](../../reference/store_commands/driver.html)**
    Issues a *Class.forName* request to load the named class.
-   **[elapsedtime](../../reference/store_commands/elapsedtime.html)**
    Displays the total time elapsed during statement execution.
-   **[execute](../../reference/store_commands/execute.html)**
    Executes a prepared statement or a SQL command with dynamic parameters.
-   **[exit](../../reference/store_commands/exit.html)**
    Completes the `snappy-shell` application and halts processing.
-   **[first](../../reference/store_commands/first.html)**
    Moves the cursor to the first row in the *ResultSet*, then fetches the row.
-   **[get scroll insensitive cursor](../../reference/store_commands/get_scroll_insensitive_cursor.html)**
    Creates a scrollable insensitive cursor with the name of the *Identifier*.
-   **[GetCurrentRowNumber](../../reference/store_commands/getcurrentrownumber.html)**
    Returns the row number for the current position of the named scroll cursor.
-   **[help](../../reference/store_commands/help.html)**
    Prints a list of the `snappy-shell` commands.
-   **[last](../../reference/store_commands/last.html)**
    Moves the cursor to the last row in the *ResultSet*, then fetches the row.
-   **[LocalizedDisplay](../../reference/store_commands/localized_display.html)**
    Specifies whether to display locale-sensitive data (such as dates) in the native format for the `snappy-shell` locale.
-   **[MaximumDisplayWidth](../../reference/store_commands/maximumdisplaywidth.html)**
    Sets the largest display width for columns to the specified value.
-   **[next](../../reference/store_commands/next.html)**
    Fetches the next row from the named cursor created with the get scroll insensitive cursor command.
-   **[prepare](../../reference/store_commands/prepare.html)**
    Creates a *java.sql.PreparedStatement* using the value of the String, accessible in `snappy-shell` by the *Identifier* given to it.
-   **[previous](../../reference/store_commands/previous.html)**
    Moves the cursor to the row previous to the current one, then fetches the row.
-   **[protocol](../../reference/store_commands/protocol.html)**
    Specifies the protocol, as a String, for establishing connections and automatically loads the appropriate driver.
-   **[relative](../../reference/store_commands/relative.html)**
    Moves the cursor to the row that is *int* number of rows relative to the current row, then fetches the row.
-   **[remove](../../reference/store_commands/remove.html)**
    Removes a previously prepared statement from gfxd.
-   **[rollback](../../reference/store_commands/rollback.html)**
    Issues a *java.sql.Connection.rollback* request.
-   **[run](../../reference/store_commands/run.html)**
    Treats the value of the string as a valid file name, and redirects `snappy-shell` processing to read from that file until it ends or an exit command is executed.
-   **[set connection](../../reference/store_commands/set_connection.html)**
    Specifies which connection to make current when more than one connection is open.
-   **[show](../../reference/store_commands/show.html)**
    Displays information about active connections and database objects.
-   **[wait for](../../reference/store_commands/wait_for.html)**
    Displays the results of a previously started asynchronous command.


