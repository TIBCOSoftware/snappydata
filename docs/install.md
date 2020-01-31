# Provisioning TIBCO ComputeDB

## Getting Started
Multiple options are provided to get started with TIBCO ComputeDB. Easiest way to get going with ComputeDB is on your laptop. You can also use any of the following options:

*	On-premise clusters

*	AWS

*	Docker
*	Kubernetes

You can download and install the latest version of TIBCO ComputeDB from [here](https://edelivery.tibco.com/storefront/index.ep). Refer to the [documentation](/install.md) for installation steps.

If you would like to build TIBCO ComputeDB from source, refer to the [documentation on building from source](/install/building_from_source.md).

You can find more information on options for running TIBCO ComputeDB [here](/quickstart.md).


### Procuring TIBCO ComputeDB Artifacts

#### Maven Dependency

TIBCO ComputeDB artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:

```
groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 1.2.0
```

#### SBT Dependency

If you are using SBT, add this line to your **build.sbt** for core TIBCO ComputeDB artifacts:

```
libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "1.2.0"
```

For additions related to TIBCO ComputeDB cluster, use:

```
libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "1.2.0"
```

You can find more specific TIBCO ComputeDB artifacts [here](http://mvnrepository.com/artifact/io.snappydata)

!!!Note
	If your project fails when resolving the above dependency (that is, it fails to download `javax.ws.rs#javax.ws.rs-api;2.1`), it may be due an issue with its pom file. </br> As a workaround, you can add the below code to your **build.sbt**:

```
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}
```

For more details, refer [https://github.com/sbt/sbt/issues/3618](https://github.com/sbt/sbt/issues/3618).

#### Quick Test to Measure Performance of TIBCO ComputeDB vs Apache Apache Spark
If you are already using Apache Spark, experience upto 20x speedup for your query performance with TIBCO ComputeDB. Try out this [test](https://github.com/SnappyDataInc/snappydata/blob/master/examples/quickstart/scripts/Quickstart.scala) using the Apache Spark Shell. 


#### Streaming Example - Ad Analytics
Here is a stream + Transactions + Analytics use case example to illustrate the SQL as well as the Apache Spark programming approaches in TIBCO ComputeDB - [Ad Analytics code example](https://github.com/SnappyDataInc/snappy-poc). Here is a [screencast](https://www.youtube.com/watch?v=bXofwFtmHjE) that showcases many useful features of TIBCO ComputeDB. The example also goes through a benchmark comparing TIBCO ComputeDB to a Hybrid in-memory database and Cassandra.


TIBCO ComputeDB offers two editions of the product, SnappyData Community Edition, and TIBCO ComputeDB Enterprise Edition.

The SnappyData Community Edition is Apache 2.0 licensed. It is a free, open-source version of the product that can be downloaded by anyone.
The Enterprise Edition includes several additional capabilities that are closed source and only available as part of a licensed subscription.

<a id= download> </a>
<heading2> Download SnappyData Community Edition</heading2>

[Download the SnappyData 1.2.0 Community Edition (Open Source)](https://github.com/SnappyDataInc/snappydata/releases/) from the release page, which lists the latest and previous releases of SnappyData. The packages are available in compressed files (.tar format).

* [**SnappyData 1.2.0 Release download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.2.0/snappydata-1.2.0-bin.tar.gz)


<heading2> Download TIBCO ComputeDB Enterprise Edition</heading2> 

1. On the [TIBCO eDelivery website](https://edelivery.tibco.com), search for **TIBCO ComputeDB** and go to the **Product Detail** page.
2. Click **Download** and then enter your credentials. 
3. In the Download page, select the version and the operation system as per your requirement.
4. Read and accept the **END USER LICENSE AGREEMENT**.
5. Choose an installation option and then click **Download**.
6. You can also download the following additional files by clicking on the links:

	* DEBIAN INSTALLER

	* REDHAT INSTALLER

	* JDBC JAR FILE

	* ODBC INSTALLERS

<a id= provisioningsnappy> </a>
<heading2>TIBCO ComputeDB Provisioning Options</heading2>

<heading3>Prerequisites</heading3>

Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the *JAVA_HOME* environment variable is set on each computer.

The following options are available for provisioning TIBCO ComputeDB:

* [On-Premise](install/install_on_premise.md) <a id="install-on-premise"></a>

* [Amazon Web Services (AWS)](install/setting_up_cluster_on_amazon_web_services.md) <a id="setting-up-cluster-on-amazon-web-services-aws"></a>

* [Kubernetes](kubernetes.md)

* [Docker](/quickstart/getting_started_with_docker_image.md)

* [Building from Source](install/building_from_source.md)<a id="building-from-source"></a>

<heading3>Configuring the Limit for Open Files and Threads/Processes</heading3>

On a Linux system, you can set the limit of open files and thread processes in the **/etc/security/limits.conf** file. 
</br>A minimum of **8192** is recommended for open file descriptors limit and **>128K** is recommended for the number of active threads. 
</br>A typical configuration used for TIBCO ComputeDB servers and leads can appear as follows:

```pre
snappydata          hard    nofile      32768
snappydata          soft    nofile      32768
snappydata          hard    nproc       unlimited
snappydata          soft    nproc       524288
snappydata          hard    sigpending  unlimited
snappydata          soft    sigpending  524288
```
* `snappydata` is the user running TIBCO ComputeDB.

Recent linux distributions using systemd (like RHEL/CentOS 7, Ubuntu 18.04) require the **NOFILE** limit to be increased in systemd configuration too. Edit **/etc/systemd/system.conf ** as root, search for **#DefaultLimitNOFILE** under the **[Manager] **section. Uncomment and change it to **DefaultLimitNOFILE=32768**. 
Reboot for the above changes to be applied. Confirm that the new limits have been applied in a terminal/ssh window with **"ulimit -a -S"** (soft limits) and **"ulimit -a -H"** (hard limits).

