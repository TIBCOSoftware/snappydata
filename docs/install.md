# Provisioning TIBCO ComputeDB

TIBCO ComputeDB offers two editions of the product, SnappyData Community Edition, and TIBCO ComputeDB Enterprise Edition.

The SnappyData Community Edition is Apache 2.0 licensed. It is a free, open-source version of the product that can be downloaded by anyone.
The Enterprise Edition includes several additional capabilities that are closed source and only available as part of a licensed subscription.

<a id= download> </a>
<heading2> Download SnappyData Community Edition</heading2>

[Download the SnappyData 1.0.2.1 Community Edition (Open Source)](https://github.com/SnappyDataInc/snappydata/releases/) from the release page, which lists the latest and previous releases of SnappyData. The packages are available in compressed files (.tar format).

* [**SnappyData 1.0.2.1 Release download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.2.1/snappydata-1.0.2.1-bin.tar.gz)

* [**SnappyData 1.0.2.1 Release (user-provided Hadoop) download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.2.1/snappydata-1.0.2.1-without-hadoop-bin.tar.gz) 


<heading2> Download TIBCO ComputeDB Enterprise Edition</heading2> 

1. From the [TIBCO eDelivery website](https://edelivery.tibco.com) go to the **Product Detail** page of TIBCO ComputeDB page. 

2. Read the END USER LICENSE AGREEMENT and click the **Agree to terms of service** option to accept it.

4. Click **Download** to download the installer (**snappydata-1.0.2.1-bin.tar.gz**).

5. You can also download the following additional files by clicking on the links:

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

