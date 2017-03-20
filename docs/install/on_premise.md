## Install On-Premise
SnappyData runs on UNIX-like systems (for example, Linux, Mac OS). With on-premises installation, SnappyData is installed and operated from your in-house computing infrastructure.

### Prerequisites
Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the _JAVA_HOME_ environment variable is set on each computer.

### Download SnappyData
Download the latest version of SnappyData from the [SnappyData Release](https://github.com/SnappyDataInc/snappydata/releases/) page, which lists the latest and previous releases of SnappyData.

The packages are available in compressed files (.zip and .tar format). On this page, you can also view details of features and enhancements introduced in specific releases.

* ** SnappyData 0.7 download link **
[(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-bin.zip)

* **SnappyData 0.7 (hadoop provided) download link** [(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-without-hadoop-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-without-hadoop-bin.zip)
<a id="singlehost"></a>
### Single Host Installation
This is the simplest form of deployment and can be used for testing and POCs.

Open the command prompt and run the following command to extract the downloaded archive file and to go the location of the SnappyData home directory. 
```bash
$ tar -xzf snappydata-0.7-bin.tar.gz   
$ cd snappydata-0.7-bin/
```
Start a basic cluster with one data node, one lead, and one locator
```
./sbin/snappy-start-all.sh
```
For custom configuration and to start more nodes,  see the section [How to Configure the SnappyData cluster](../configuration.md)

### Multi-Host Installation
For real life use cases, you need multiple machines on which SnappyData can be deployed. You can start one or more SnappyData node on a single machine based on your machine size.

### Machines with a Shared Path
If all your machines can share a path over an NFS or similar protocol, then follow the steps below:

#### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP address of each SnappyData member machine.

* Ensure that SSH is supported and you have configured all machines to be accessed by [passwordless SSH](../configuring_cluster/configuring_ssh_without_password.md).

#### Steps to Set up the Cluster

1. Copy the downloaded binaries to the shared folder.

2. Extract the downloaded archive file and go to SnappyData home directory.

		$ tar -xzf snappydata-0.7-bin.tar.gz 
		$ cd snappydata-0.7-bin/
 
3. Configure the cluster as described in [How to Configure SnappyData cluster](../../configuring_cluster/configuration_files.md).

4. After configuring each of the components, run the `snappy-start-all.sh` script:

		./sbin/snappy-start-all.sh 

This creates a default folder named **work** and stores all SnappyData member's artifacts separately. Each member folder is identified by the name of the node.

If SSH is not supported then follow the instructions in the Machines without a Shared Path section.

### Machines without a Shared Path 

#### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP Address of each SnappyData member machine.

* On each host machine, create a new member working directory for each SnappyData member, that you want to run the host. <br> The member working directory provides a default location for the log, persistence, and status files for each member, and is also used as the default location for locating the member's configuration files.
<br>For example, if you want to run both a locator and server member on the local machine, create separate directories for each member.

#### To Configure the Cluster
1. Copy and extract the downloaded binaries on each machine

2. Individually configure and start each member

<Note> Note: We are providing all configuration parameter as command line arguments rather than reading from a conf file.</Note>

The example below starts a locator and server.

```bash 
$ bin/snappy-shell locator start  -dir=/node-a/locator1 
$ bin/snappy-shell server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy-shell locator stop
$ bin/snappy-shell server stop
``` 
