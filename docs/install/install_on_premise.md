<a id="install-on-premise"></a>
# Install On-Premise

SnappyData runs on UNIX-like systems (for example, Linux, Mac OS). With on-premises installation, SnappyData is installed and operated from your in-house computing infrastructure.

<a id="singlehost"></a>
## Single-Host installation
This is the simplest form of deployment and can be used for testing and POCs.

Open the command prompt, go the location of the downloaded SnappyData file, and run the following command to extract the archive file.

```bash
$ tar -xzf snappydata-<version-number>bin.tar.gz
$ cd snappydata-<version-number>-bin/
```
Start a basic cluster with one data node, one lead, and one locator
```
./sbin/snappy-start-all.sh
```
For custom configuration and to start more nodes,  see the section on [configuring the SnappyData cluster](../configuring_cluster/configuring_cluster.md).

## Multi-Host installation
For real-life use cases, you need multiple machines on which SnappyData can be deployed. You can start one or more SnappyData node on a single machine based on your machine size.

## Machines with a shared path
If all your machines can share a path over an NFS or similar protocol, then follow the steps below:

#### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP address of each SnappyData member machine.

* Ensure that SSH is supported and you have configured all machines to be accessed by [passwordless SSH](../reference/misc/passwordless_ssh.md).

### Steps to setup the cluster

1. Copy the downloaded binaries to the shared folder.

2. Extract the downloaded archive file and go to SnappyData home directory.

		$ tar -xzf snappydata-<version-number>-bin.tar.gz
		$ cd snappydata-<version-number>.-bin/

3. Configure the cluster as described in [Configuring the Cluster](../configuring_cluster/configuring_cluster.md).

4. After configuring each of the components, run the `snappy-start-all.sh` script:

		./sbin/snappy-start-all.sh

This creates a default folder named **work** and stores all SnappyData member's artifacts separately. The folder is identified by the name of the node.

If SSH is not supported then follow the instructions in the [Machines without a Shared Path](#machine-shared-path) section.

<a id="machine-shared-path"></a>
## Machines without a shared path

### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP Address of each SnappyData member machine.

* On each host, create a working directory for each SnappyData member, that you want to run on the host. <br> The member working directory provides a default location for the log, persistence, and status files for that member.
<br>For example, if you want to run both a locator and server member on the local machine, create separate directories for each member.

### To configure the cluster
1. Copy and extract the downloaded binaries on each machine

2. Individually configure and start each member

!!! Note: 
	All configuration parameters are provided as command line arguments rather than reading from a configuration file.

The example below starts a cluster by individually launching locator, server and lead processes.

```bash
$ bin/snappy locator start  -dir=/node-a/locator1
$ bin/snappy server start  -dir=/node-b/server1  -locators:localhost:10334
$ bin/snappy leader start  -dir=/node-c/lead1  -locators:localhost:10334

$ bin/snappy locator stop -dir=/node-a/locator1
$ bin/snappy server stop -dir=/node-b/server1
$ bin/snappy leader stop -dir=/node-c/lead1top
```
