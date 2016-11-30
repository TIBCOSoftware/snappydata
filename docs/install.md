## System Requirements and Supported Configuration

|Operating System | Version | Java Virtual Machine (JVM)| Support (Production/Developer) |
| ---------------------- |:---------------------- |:---------------------- |:---------------------- |
|Red Hat EL 5, 6.2, 6.4 |x86 (64bit) |Java SE 1.8.0  |Production |
|CentOS 6.2, 6.4, 7|  x86 (64bit)| Java SE 1.8.0 |Production |
|Ubuntu 14.04|x86 (64bit and 32 bit)  | Java SE 1.8.0 | Production|
|SUSE Linux Enterprise Server 11|x86 (64bit and 32 bit)  | Java SE 1.8.0 | Production|
|Ubuntu 10.11 |x86 (64bit and 32 bit)  | Java SE 1.8.0 | Developer|
|MAC OS X |<TO BE CONFIRMED> | Java SE 1.8.0 | Developer|

## Set up SnappyData 
TO BE DONE

### Set Up SnappyData Cluster Locally 
The files available for download are:

|File Name  |Description |
| ---------------------- |:---------------------- |
|**File Name**   | Contains the __files|
|**File Name**   | Contains the __files|

> Note: : `*.*`  represents the file version number. The list of files may differ for each release.

### Setting Up SnappyData Cluster with Amazon Webservices (AWS)
TO BE DONE

### Set Up SnappyData Cluster with Azure
TO BE DONE

### Set Up SnappyData Cluster with Docker
TO BE DONE

## Obtaining the Installation Files
### Downloading the Installation Files (Compressed File)
Download the latest version of SnappyData from the [SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/)  page, which lists the latest and previous releases of SnappyData. 
The packages are available in compressed files (.zip and .tar format). You can also view details of features and enhancements introduced in specific releases.

> Note: It is recommended that you download and install the latest version of SnappyData.

### Building SnappyData (Latest Release Branch)
Using this option you can clone the latest installation package from GitHub. It includes the latest installation packages  containing the changes or code updates that have been made in between releases.
The following command creates a local copy of the repository. Existing versions on your system, it is updated with the latest version. For more instruction on building, layout of the code, integration with IDEs using Gradle, etc refer to [building from source, project layout](#build-instructions.md).

**Master development branch**

```
git clone https://github.com/SnappyDataInc/snappydata.git --recursive
\######\# 0.x preview release branch with stability and other fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.x-preview --recursive
```
> NOTE: SnappyData is built using Spark 1.6 (build xx) which is packaged as part of SnappyData. While you can build your application using Apache Spark 1.5, you will need to link to Snappy-spark to make use of the SnappyData extensions. Gradle build tasks are packaged.

## Start the Installation
### Prerequisites
Before you start the installation, make sure that the following prerequisites are met. If you fail to do so, the installation may not be successful.

*Java SE Development Kit 8 is installed, and the  _JAVA_HOME_ environment variable is set on each computer.
*(TO BE DONE) - Additional prerequisites)

#### Installation Process (TO BE DONE/CONFIRMED)
SnappyData Installation Methods (Multi-Host Installation/Single Installation/NFS Installation)

> Note: NFS installation is recommended for SnappyData installation, as you need to set up and maintain only one system

Enable SSH without password across your cluster, as SnappyData scripts use SSH to start components.
Update the configuration files that specify the machine name and properties for the lead node, locator and servers


>Note: 
* Ensure that SnappyData is installed on all required computers.
* All computers in the cluster must be accessible at all times.


### Step 1: Configuring SSH Login without Password
By default, Secure Socket Shell (SSH) requires a password for authentication on a remote server. 
This setting needs to be modified to allow you to login to the remote host through the SSH protocol, without having to enter your SSH password multiple times when working with SnappyData. 

To install and configure SSH, do the following:

1. **Install SSH** <br>
	To install SSH,  type: 
    `sudo apt-get install ssh` 
    Mac OS X has a built-in SSH client. 

2. **Generate an RSA key pair**<br>
    To generate an RSA key pair run the following command on the client computer,
    `ssh-keygen -t rsa`
    Press Enter when prompted to enter file in which to save the key, and for the passpharase.

3.  **Copy the Public Key**<br>
    Once the key pair is generated, copy the contents of the public key file, to the authorized key on the remote 	site, by typing
    `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`

### Step 2: Configuring SnappyData Cluster
SnappyData, a database server cluster, has three main components, Locator, Server and Lead. These component can be configured individually using configuration files.

> Note: If the directory and properties are not specified, a default directory is created inside the SPARK_HOME directory.

#### Configuring Locators

Locators provide discovery service for the cluster. It informs a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.
In this file, you can specify:
* The host name on which a SnappyData lead is started 
* The startup directory where the logs and configuration files for that lead instance are located 
* SnappyData specific properties that can be passed

Create the configuration files (conf/locators) for locators in `SNAPPY_HOME`.

#### Configuring Leads

Lead Nodes act as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.

Create the configuration files (conf/leads) for leads in `SNAPPY_HOME`.

#### Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node, or pass it to the lead node for execution by Spark SQL.
Create the configuration files (conf/servers) for data servers in `SNAPPY_HOME`.


> Note: The following rules apply when modifying the configuration files:_
* Provide the hostnames of the nodes where you intend to start the member
* Ensure that one node name is provided per line


#### Configuring Additional SnappyData Properties
|Property  |Description |
| ---------------------- |:---------------------- |
|peer-discovery-port|Locator specific property. This is the port on which the locator listens for member discovery.<br>The default value is 10334. |
|client-port |Port that a member listens on for client connections. |
|locators|List the locators as comma-separated host:port values.  <br> For locators, the list must include all other locators in use. For Servers and Leads, the list must include all the locators of the distributed system. |
|dir|Details of the working directory. The member working directory provides a default location for log, persistence, and status files for each member. If not specified, a member directory is created in `SNAPPY_HOME\work.` |
|classpath|Provide application specific code to the lead and servers. The application jars have to be provided during startup. |
|heap-size|Set a fixed heap size and for the Java Virtual Machine. |
|J|Use this to configure any JVM specific property. <br>For example:. -J-XX:MaxPermSize=512m. |
