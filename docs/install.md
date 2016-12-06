## Install On Premise
### Single Host Installation

SnappyData runs on UNIX-like systems (e.g. Linux, Mac OS).
Also make sure that Java SE Development Kit 7 or later version is installed, and the  _JAVA_HOME_ environment variable is set on each computer.

Download the latest version of SnappyData from the
[SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/)
 page, which lists the latest and previous releases of SnappyData.

```
tar -xvf <snappy_binaries>
cd snappy
```

To start a basic cluster with one data node, one lead and one locator

```
./sbin/snappy-start-all.sh
```

For custom configuration see the section [How to Configure SnappyData cluster](#Configure_Cluster)

### Multi Host Installation

#### Machines with shared path
If all your machines can share a path over NFS or similar protocol the you can follow the below steps.

Download the latest version of SnappyData from the
[SnappyData Release Page](https://github.com/SnappyDataInc/snappydata/releases/)
page, which lists the latest and previous releases of SnappyData.

Copy the downloaded binaries in the shared folder.

```
tar -xvf <snappy_binaries>
cd snappy
```
If SSH is supported simply configure the cluster as per [How to Configure SnappyData cluster](#Configure_Cluster)

If SSH is not supported the copy the binaries.
Explain all steps

#### Machines without shared path

## Setting up Cluster on AWS
To Be done

## Setting up Cluster on Azure
To Be done

## Setting up Cluster with Docker images
To Be done

## Building from source
To Be done

<a id="Configure_Cluster"></a>
### Configuring SnappyData Cluster(Ignore for the time being)
SnappyData, a database server cluster, has three main components, Locator, Server and Lead. These component can be configured individually using configuration files.

> Note: If the directory and properties are not specified, a default directory is created inside the SPARK_HOME directory.

##### Configuring Locators

Locators provide discovery service for the cluster. It informs a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.
In this file, you can specify:
* The host name on which a SnappyData lead is started
* The startup directory where the logs and configuration files for that lead instance are located
* SnappyData specific properties that can be passed

Create the configuration files (conf/locators) for locators in `SNAPPY_HOME`.

##### Configuring Leads

Lead Nodes act as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance, but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.

Create the configuration files (conf/leads) for leads in `SNAPPY_HOME`.

##### Configuring Data Servers
Data Servers hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node, or pass it to the lead node for execution by Spark SQL.
Create the configuration files (conf/servers) for data servers in `SNAPPY_HOME`.


> Note: The following rules apply when modifying the configuration files:_
* Provide the hostnames of the nodes where you intend to start the member
* Ensure that one node name is provided per line


##### Configuring Additional SnappyData Properties
|Property  |Description |
| ---------------------- |:---------------------- |
|peer-discovery-port|Locator specific property. This is the port on which the locator listens for member discovery.<br>The default value is 10334. |
|client-port |Port that a member listens on for client connections. |
|locators|List the locators as comma-separated host:port values.  <br> For locators, the list must include all other locators in use. For Servers and Leads, the list must include all the locators of the distributed system. |
|dir|Details of the working directory. The member working directory provides a default location for log, persistence, and status files for each member. If not specified, a member directory is created in `SNAPPY_HOME\work.` |
|classpath|Provide application specific code to the lead and servers. The application jars have to be provided during startup. |
|heap-size|Set a fixed heap size and for the Java Virtual Machine. |
|J|Use this to configure any JVM specific property. <br>For example:. -J-XX:MaxPermSize=512m. |

##### Configuring SSH Login without Password
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