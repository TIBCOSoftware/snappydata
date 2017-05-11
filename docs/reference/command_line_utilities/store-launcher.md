# Command Line Utitilites

Use the *snappy* command-line utility to launch SnappyData utilities.

To display a full list of snappy commands and options:

``` pre
snappy --help
```

The command form to display a particular utility's usage is:

``` pre
snappy <utility> --help
```

With no arguments, `snappy` starts an [interactive SQL command shell](../../reference/interactive_commands/store_command_reference.md):

``` pre
snappy
```

To specify a system property for an interactive `snappy` session, you must define the JAVA_ARGS environment variable before starting `snappy`. For example, `snappy` uses the `gfxd.history` system property to define the file that stores a list of the commands that are executed during an interactive session. To change this property, you would define it as part of the JAVA_ARGS variable:

``` pre
$ export JAVA_ARGS="-Dgfxd.history=/Users/yozie/snappystore-history.sql" 
$ snappy
```

To launch and exit a `snappy` utility (rather than start an interactive `snappy` shell) use the syntax:

``` pre
snappy <utility> <arguments for specified utility>
```

To specify a system property when launching a `snappy` utility, use -J-D*property_name*=*property_value* argument.

In addition to launching various utilities provided with SnappyData, when launched without any arguments, `snappy` starts an interactive command shell that you can use to connect to a SnappyData system and execute various commands, including SQL statements.

The launcher honors the current CLASSPATH environment variable and adds it to the CLASSPATH of the utility or command shell being launched. To pass additional arguments to the JVM, set the `JAVA_ARGS` environment variable when invoking the *gfxd* script.

!!!Note:
	The `JAVA_ARGS` environment variable does not apply to the `snappy server` and `snappy locator` tools that launch a separate background process. To pass Java properties to those tools, use the `-J` option as described in the help for those tools. </p>

The launcher uses the `java` executable that is found in the PATH. To override this behavior, set the <mark> TO VERIFY `GFXD_JAVA`</mark> environment variable to point to the desired Java executable. (note the supported JRE versions in [Supported Configurations and System Requirements](../../sys_requirement.md).

-   **[backup](../../reference/command_line_utilities/store-backup.md)**
    Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.

-   **[compact-all-disk-stores](../../reference/command_line_utilities/store-compact-all-disk-stores.md)**
    Perform online compaction of SnappyData disk stores.

-   **[compact-disk-store](../../reference/command_line_utilities/store-compact-disk-store.md)**
    Perform offline compaction of a single SnappyData disk store.

-   **[list-missing-disk-stores](../../reference/command_line_utilities/store-list-missing-disk-stores.md)**
    Lists all disk stores with the most recent data for which other members are waiting.

-   **[locator](../../reference/command_line_utilities/store-locator.md)**
    Allows peers (including other locators) in a distributed system to discover each other without the need to hard-code anything about other peers.

-   **[print-stacks](../../reference/command_line_utilities/store-print-stacks.md)**
    Prints a stack dump of SnappyData member processes.

-   **[revoke-missing-disk-store](../../reference/command_line_utilities/store-revoke-missing-disk-stores.md)**
    Instruct SnappyData members to stop waiting for a disk store to become available.

-   **[run](../../reference/command_line_utilities/store-run.md)**
    Connects to a SnappyData distributed system and executes the contents of a SQL command file. All commands in the specified file must be compatible with the interactive gfxd shell.

-   **[server](../../reference/command_line_utilities/store-server.md)**
    A SnappyData server is the main server side component in a SnappyData system that provides connectivity to other servers, peers, and clients in the cluster. It can host data. A server is started using the *server* utility of the *gfxd* launcher.

-   **[show-disk-store-metadata](../../reference/command_line_utilities/store-show-disk-store-metadata.md)**
    Display the disk store metadata for a specified disk store directory.

-   **[version](../../reference/command_line_utilities/store-version.md)**
    Prints information about the SnappyData product version.
