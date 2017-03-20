# snappy-shell Launcher Commands

Use the *gfxd* command-line utility to launch RowStore utilities.

-   <a href="store-launcher.html#reference_9518856325F74F79B13674B8E060E6C5__section_7C703C6F85BE4B3B95BC9059DF885BED" class="xref">Syntax</a>
-   <a href="store-launcher.html#reference_9518856325F74F79B13674B8E060E6C5__section_E9CEB9D7B99D4621A1A8ADA28A49670A" class="xref">Description</a>

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_7C703C6F85BE4B3B95BC9059DF885BED"></a>

##Syntax

!!! Note:
	Although RowStore introduces the `snappy-shell` utility to replace the earlier `sqlf` utility, "sqlf" is still provided and supported as an optional syntax in this release for convenience.</p>
To display a full list of snappy-shell commands and options:

``` pre
snappy-shell --help
```

The command form to display a particular utility's usage is:

``` pre
snappy-shell <utility> --help
```

With no arguments, `snappy-shell` starts an <a href="store_command_reference.html#concept_15619CF8E8494962BE296C1BC976D2B3" class="xref noPageCitation" title="gfxd implements an interactive command-line tool that is based on the Apache Derby ij tool. Use gfxd to run scripts or interactive queries against a RowStore cluster.">interactive SQL command shell</a>:

``` pre
snappy-shell
```

To specify a system property for an interactive `snappy-shell` session, you must define the JAVA\_ARGS environment variable before starting `snappy-shell`. For example, `snappy-shell` uses the `snappy-shell.history` system property to define the file that stores a list of the commands that are executed during an interactive session. To change this property, you would define it as part of the JAVA\_ARGS variable:

``` pre
$ export JAVA_ARGS="-Dgfxd.history=/Users/yozie/snappystore-history.sql"
$ snappy
```

To launch and exit a `snappy-shell` utility (rather than start an interactive `snappy-shell` shell) use the syntax:

``` pre
snappy-shell <utility> <arguments for specified utility>
```

In this command form, *&lt;utility&gt;* is one of the following.




To specify a system property when launching a `snappy-shell` utility, use -J-D*property\_name*=*property\_value* argument.

<a id="reference_9518856325F74F79B13674B8E060E6C5__section_E9CEB9D7B99D4621A1A8ADA28A49670A"></a>

##Description

In addition to launching various utilities provided with RowStore, when launched without any arguments, `snappy-shell` starts an interactive command shell that you can use to connect to a RowStore system and execute various commands, including SQL statements.

The launcher honors the current CLASSPATH environment variable and adds it to the CLASSPATH of the utility or command shell being launched. To pass additional arguments to the JVM, set the `JAVA\_ARGS` environment variable when invoking the *gfxd* script.

<p class="note"><strong>Note:</strong> The `JAVA\_ARGS` environment variable does not apply to the `snappy-shell rowstore server` and `snappy-shell rowstore locator` tools that launch a separate background process. To pass Java properties to those tools, use the `-J` option as described in the help for those tools. </p>

The launcher uses the `java` executable that is found in the PATH. To override this behavior, set the `GFXD\_JAVA` environment variable to point to the desired Java executable. (note the supported JRE versions in <a href="../../getting_started/topics/system_requirements.html#concept_system-requirements" class="xref" title="This topic describes the supported configurations and system requirements for RowStore.">Supported Configurations and System Requirements</a>).

-   **[backup](../../reference/store_commands/store-backup.html)**
    Creates a backup of operational disk stores for all members running in the distributed system. Each member with persistent data creates a backup of its own configuration and disk stores.
-   **[compact-all-disk-stores](../../reference/store_commands/store-compact-all-disk-stores.html)**
    Perform online compaction of RowStore disk stores.
-   **[compact-disk-store](../../reference/store_commands/store-compact-disk-store.html)**
    Perform offline compaction of a single RowStore disk store.
-   **[encrypt-password](../../reference/store_commands/store-encrypt-password.html)**
    Generates an encrypted password string for use in the <span class="ph filepath">gemfirexd.properties</span> file when configuring BUILTIN authentication, or when accessing an external data source with an AsyncEventListener implementation or DBsynchronizer configuration.
-   **[install-jar](../../reference/store_commands/store-install-jar.html)**
    Installs a JAR file and automatically loads the JAR file classes into the RowStore class loader. This makes the JAR classes available to all members of the distributed system (including members that join the system at a later time).
-   **[list-missing-disk-stores](../../reference/store_commands/store-list-missing-disk-stores.html)**
    Lists all disk stores with the most recent data for which other members are waiting.
-   **[locator](../../reference/store_commands/store-locator.html)**
    Allows peers (including other locators) in a distributed system to discover each other without the need to hard-code anything about other peers.
-   **[Logging Support](../../reference/store_commands/store-logging.html)**
    You can specify JDBC boot properties with `snappy-shell rowstore server` and `snappy-shell rowstore locator` commands to configure the log file location and log severity level for RowStore servers and locators, respectively.
-   **[merge-logs](../../reference/store_commands/store-merge-logs.html)**
    Merges multiple log files into a single log.
-   **[print-stacks](../../reference/store_commands/store-print-stacks.html)**
    Prints a stack dump of RowStore member processes.
-   **[remove-jar](../../reference/store_commands/store-remove-jar.html)**
    Removes a JAR file installation, unloaded all classes associated with the JAR.
-   **[replace-jar](../../reference/store_commands/store-replace-jar.html)**
    Replaces an installed JAR file with the contents of a new JAR file. The classes in the new JAR file are automatically loaded into the RowStore class loader and they replace any classes that were previously installed for the same JAR identifier. RowStore also recompiles objects that depend on the JAR file, such as installed listener implementations.
-   **[revoke-missing-disk-store](../../reference/store_commands/store-revoke-missing-disk-stores.html)**
    Instruct RowStore members to stop waiting for a disk store to become available.
-   **[run](../../reference/store_commands/store-run.html)**
    Connects to a RowStore distributed system and executes the contents of a SQL command file. All commands in the specified file must be compatible with the interactive gfxd shell.
-   **[server](../../reference/store_commands/store-server.html)**
    A RowStore server is the main server side component in a RowStore system that provides connectivity to other servers, peers, and clients in the cluster. It can host data. A server is started using the *server* utility of the *gfxd* launcher.
-   **[show-disk-store-metadata](../../reference/store_commands/store-show-disk-store-metadata.html)**
    Display the disk store metadata for a specified disk store directory.
-   **[shut-down-all](../../reference/store_commands/store-shut-down-all.html)**
    Instructs all RowStore accessor and data store members to disconnect from the distributed system and shut down.
-   **[stats](../../reference/store_commands/store-stats.html)**
    Displays statistics values from the statistics archive.
-   **[upgrade-disk-store](../../reference/store_commands/store-upgrade-disk-store.html)**
    (Not supported in this release.) Upgrades disk stores to the current version of RowStore.
-   **[validate-disk-store](../../reference/store_commands/store-validate-disk-store.html)**
    Verifies the health of an offline disk store and provides information about the tables using that disk store.
-   **[version](../../reference/store_commands/store-version.html)**
    Prints information about the RowStore product version.
-   **[write-data-dtd-to-file](../../reference/store_commands/store-write-data-dtd-to-file.html)**
    Creates a Document Type Definition (DTD) file that specifies the layout of an XML data file created using `snappy-shell write-data-to-xml`.
-   **[write-data-to-db](../../reference/store_commands/store-write-data-to-db.html)**
    Inserts data into a database using one or more data XML files (created with snappy-shell write-data-to-xml), and having the database schema defined in one or more schema XML files (created with snappy-shell write-schema-to-xml). This command is generally used with a RowStore cluster to export table data, but it can also be used with other JDBC datasources.
-   **[write-data-to-xml](../../reference/store_commands/store-write-data-to-xml.html)**
    Writes the data of all of the tables in a database to an XML file. (You can use the snappy-shell write-data-dtd-to-file command to create a Document Type Definition (DTD) file that describes the layout of data in the XML file.) The resulting XML file can be used to re-load the data into the tables, either in RowStore or in another database management system. This command is generally used with a RowStore cluster to export table data, but it can also be used with other JDBC datasources.
-   **[write-schema-to-db](../../reference/store_commands/store-write-schema-to-db.html)**
    Creates a schema in a database by using the contents of a one or more schema XML files (see snappy-shell write-schema-to-xml). This command is generally used with a RowStore cluster to export the schema, but it can also be used with other JDBC datasources.
-   **[write-schema-to-sql](../../reference/store_commands/store-write-schema-to-sql.html)**
    Writes the schema of a database to a file as SQL commands. You can use the resulting file to recreate the schema in RowStore, or in another database management system. This command is generally used with a RowStore cluster to export the schema, but it can also be used with other JDBC datasources.
-   **[write-schema-to-xml](../../reference/store_commands/store-write-schema-to-xml.html)**
    Writes the schema of a database to an XML file. You can use the resulting XML file to recreate the schema in RowStore, or in another database management system. This command is generally used with a RowStore cluster to export the schema, but it can also be used with other JDBC datasources.


