# Migration Guide

This guide provides information related to the migration of systems running an earlier version of SnappyData to the latest version of SnappyData. We assume that you have SnappyData already installed, and you are migrating to the latest version of SnappyData.

Before you begin migrating, ensure that you understand the new features and any specific requirements for that release. 

For more information see,


<!--

* [Manually Upgrading from GemFire XD 1.4.x to SnappyData RowStore 1.5](upgrade_gemfirexd_standalone.md)

* [Manually Upgrading from SQLFire 1.1.x to SnappyData RowStore 1.5](upgrade_sqlfire_standalone.md)
-->

* [Migrating from SnappyData version 0.8 to version 0.9](migration-0.8-0.9.md)

<!--* [Migrating from SnappyData version 0.9 to version 0.9](migration-0.9-01_rc.md)-->

**Before you begin migration**:

1. Backup the existing environment: Make sure you create a backup of the locator, lead, and server configuration files that exist in the **conf** folder located in the SnappyData home directory. 

2. Stop the cluster and verify that all members are stopped: You can shutdown the cluster using the `sbin/snappy-stop-all.sh` command. To ensure that all the members have been shut down correctly, use the `sbin/snappy-status-all.sh` command.
	
3. Re-install SnappyData: After you have stopped the cluster, [install the latest version of SnappyData](../install/index.md).

4. Reconfigure your cluster.
