# Upgrade Instructions

This guide provides information for upgrading systems running an earlier version of SnappyData. We assume that you have SnappyData already installed, and you are upgrading to the latest version of SnappyData.

Before you begin the upgrade, ensure that you understand the new features and any specific requirements for that release.

<a id="pre-upgrade-steps"></a>
## Before You Upgrade

1. Confirm that your system meets the hardware and software requirements described in [System Requirements](../install/system_requirements.md) section.

2. Backup the existing environment: </br>Create a backup of the locator, lead, and server configuration files that exist in the **conf** folder located in the SnappyData home directory.

3. Stop the cluster and verify that all members are stopped: You can shut down the cluster using the `sbin/snappy-stop-all.sh` command. </br>To ensure that all the members have been shut down correctly, use the `sbin/snappy-status-all.sh` command.

4. Create a [backup of the operational disk store files](../reference/command_line_utilities/store-backup.md) for all members in the distributed system.

5. Reinstall SnappyData: After you have stopped the cluster, [install the latest version of SnappyData](../install/index.md).

6. Reconfigure your cluster using the locator, lead, and server configuration files you backed up in step 1.

7. To ensure that the restore script (restore.sh) copies files back to their original locations, make sure that the
   disk files are available at the original location. Follow the relevant steps from the next two sections before
   restarting the cluster with the latest version of SnappyData.


## Upgrading to SnappyData 1.3.1 from 1.0.1 or Earlier Versions

When you upgrade to SnappyData 1.3.1 from product version 1.0.1 or earlier versions, it is recommended to save all the table data as parquet files, recreate the tables in the new cluster, and then load data from the saved parquet files. Before taking the backup ensure that no operations are currently running on the system. Ensure to cleanup the data from the previous cluster and start the new cluster from a clean directory.

For example:

```
# Creating parquet files in older product version 1.0.1 or prior:
snappy> create external table table1Parquet using parquet options (path '<path-to-parquet-file>') as select * from table1;
snappy> drop table table1;
snappy> drop table table1Parquet;

# Creating tables from parquet files in SnappyData 1.3.1
snappy> create external table table1_parquet using parquet options (path '<path-to-parquet-file>') ;
snappy> create table table1(...);
snappy> insert into table1 select * from table1_parquet;
```
Use a path for the Parquet file that has enough space to hold the table data. Once the re-import has completed successfully, make sure that the Parquet files are deleted explicitly.


<a id="upgrade-from-1.3.0-or-older"></a>
## Additional Steps for Upgrading to SnappyData 1.3.1 from 1.3.0 or Earlier Versions

After performing the [pre-upgrade steps](#pre-upgrade-steps), users may need to perform one or more of the steps
below before restarting the cluster due to the move from Log4j 1 to Log4j 2:

* If the existing installation is using a custom `log4j.properties`/`log4j.xml` configuration in **conf**
  directory, then you will need to convert it to an equivalent `log4j2.properties`/`log4j2.xml` configuration.
  More details can be found in the [Log4j documentation](https://logging.apache.org/log4j/2.x/manual/migration.html).
  For convenience, a template configuration is included in the product as `conf/log4j2.properties.template`
  which is equivalent of the `conf/log4j.properties.template` configuration included in previous releases
  and is also the default configuration used by the product (apart from the log file name).
  Hence, if your existing configuration was created by modifying the `conf/log4j.properties.template` file
  from the previous release, then it will be easier to find the differences between the two and just migrate
  those changes to the new `conf/log4j2.properties` copied from `conf/log4j2.properties.template` instead of
  migrating the entire configuration.

* If code of a [job](../programming_guide/snappydata_jobs.md) or its dependencies is using Log4j 1,
  then those should be migrated to use Log4j 2. It is recommended that user's job code should use SLF4J
  for logging instead of directly using Log4j. Scala code can extend `org.apache.spark.Logging` trait
  for convenience which provides methods like `logInfo`/`logError`/`logDebug` etc.

* If the code of a [UDF or UDAF](../programming_guide/udf_and_udaf.md) or its dependencies is using Log4j 1,
  then those should likewise be migrated to use Log4j 2 or preferably SLF4J. Like above scala code can use
  the `org.apache.spark.Logging` trait for convenience. To upgrade a `UDF` one will need one to drop it first
  then recreate with the new jar.

* Likewise, if a [DEPLOY JAR or PACKAGE](../reference/sql_reference/deploy.md) depends on Log4j 1,
  then it should be migrated to use Log4j 2 or preferably SLF4J or the `org.apache.spark.Logging` trait.
  Undeploy and deploy the updated jar/package for the change to take effect.

* In the worst case where one or more dependencies for any of the above three cases have a hard dependency on
  Log4j 1 which cannot be changed to use Log4j 2, you can have a `log4j.properties`/`log4j.xml` packaged in
  the jar or in product **conf** directory which configures logging for only those components. The log file
  used must be distinct from any of the others and especially the product log file to avoid any conflicts with
  Log4j 2 loggers. Furthermore, Log4j 1 itself must be included in the deployed jars directly or indirectly
  since the product no longer includes the Log4j 1 jar.


## Migrating to the SnappyData ODBC Driver

If you have been using TIBCO ComputeDB™ ODBC Driver from 1.2.0 or earlier releases, then you can install the
SnappyData ODBC Driver alongside it without any conflicts. The product name as well as the ODBC Driver name
for the two are `TIBCO ComputeDB` and `SnappyData` respectively that do not overlap with one another.
The older drivers and their DSNs will continue to work against SnappyData 1.3.1 without any changes.

While the older driver will continue to work with the new SnappyData releases, migrating to the new SnappyData
ODBC Driver is highly recommended in order to avail the new features and bug fixes over the TIBCO ComputeDB™ ODBC
Driver releases. This includes new options `AutoReconnect`, `CredentialManager` and `DefaultSchema`, support for
APIs SQLCancel/SQLCancelHandle, fixes to APIs like SQLPutData, SQLBindParameter, SQLGetDiagRec, SQLGetDiagField
and SQLGetInfo, updated dependencies among others. See the ODBC Driver changes in
[Release Notes for 1.3.0](https://tibcosoftware.github.io/snappydata/1.3.0/release_notes/release_notes/#odbc-driver)
for more details.

Migration to the SnappyData ODBC Driver will involve installation from the new MSI installer
(see [docs](../howto/connect_using_odbc_driver.md)), then creating new DSNs that have their keys and values
copied from the previous ones. While just changing the DSN driver to `SnappyData` in the registry will work,
it is recommended to create it afresh with the ODBC Setup UI that will allow setting the new options easily.
