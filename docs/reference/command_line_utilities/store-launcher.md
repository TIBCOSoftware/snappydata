# Command Line Utilities

Use the *snappy* command-line utility to launch TIBCO ComputeDB utilities.

To display a full list of Snappy commands and options:

```pre
./bin/snappy --help
```

The command form to display a particular utility's usage is:

```pre
./bin/snappy <utility> --help
```

With no arguments, `snappy` starts an [interactive SQL command shell](../../reference/interactive_commands/store_command_reference.md):

```pre
./bin/snappy



To specify a system property for an interactive `snappy` session, you must define the JAVA_ARGS environment variable before starting `snappy`. For example, `snappy` uses the `snappy.history` system property to define the file that stores a list of the commands that are executed during an interactive session. To change this property, you would define it as part of the JAVA_ARGS variable:

```pre
$ export JAVA_ARGS="-Dsnappy.history=/Users/user1/snappystore-history.sql"
$ snappy
```

To launch and exit a `snappy` utility (rather than start an interactive Snappy shell) use the syntax:

```pre
./bin/snappy <utility> <arguments for specified utility>
```

To specify a system property when launching a `snappy` utility, use -J-D*property_name*=*property_value* argument.

In addition to launching various utilities provided with TIBCO ComputeDB, when launched without any arguments, `snappy` starts an interactive command shell that you can use to connect to a TIBCO ComputeDB system and execute various commands, including SQL statements.

-   **[backup and restore](../../reference/command_line_utilities/store-backup.md)**
    Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.

-   **[compact-all-disk-stores](store-compact-all-disk-stores.md)** Performs online compaction of TIBCO ComputeDB disk stores.

-   **[compact-disk-store](store-compact-disk-store.md)** Performs offline compaction of a single TIBCO ComputeDB disk store.
<!--**[list-missing-disk-stores](../../reference/command_line_utilities/store-list-missing-disk-stores.md)**
    Lists all disk stores with the most recent data for which other members are waiting.-->

-   **[revoke-missing-disk-store](../../reference/command_line_utilities/store-revoke-missing-disk-stores.md)**
    Instruct TIBCO ComputeDB members to stop waiting for a disk store to become available.

-   **[run](../../reference/command_line_utilities/store-run.md)**
    Connects to a TIBCO ComputeDB distributed system and executes the contents of a SQL command file. All commands in the specified file must be compatible with the interactive Snappy Shell.

-   **[version](../../reference/command_line_utilities/store-version.md)**
    Prints information about the TIBCO ComputeDB product version
