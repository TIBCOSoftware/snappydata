# Provisioning SnappyData

SnappyData offers two editions of the product:

*	**Community Edition**
*	**Enterprise Edition**

The SnappyData **Community Edition** is Apache 2.0 licensed. It is a free, open-source version of the product that can be downloaded by anyone.
The **Enterprise Edition** of the product, which is sold by TIBCO Software under the name **TIBCO ComputeDB™**, includes everything that is offered in the Community Edition along with additional capabilities that are closed source and only available as part of a licensed subscription.

For more information on the capabilities of the Community Edition and Enterprise Edition, see [Community Edition (Open Source)/Enterprise Edition components](additional_files/open_source_components.md).

<a id= download> </a>
<heading2>Download SnappyData Community Edition</heading2>


Download the [SnappyData 1.2.0 Community Edition (Open Source)](https://github.com/SnappyDataInc/snappydata/releases/) from the release page, which lists the latest and previous releases of SnappyData. The packages are available in compressed files (.tar format).

* [**SnappyData 1.2.0 Release download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.2.0/snappydata-1.2.0-bin.tar.gz)


<heading2>Download SnappyData Enterprise Edition</heading2> 


You can download the Enterprise Edition from [TIBCO eDelivery website](https://edelivery.tibco.com).

<a id= provisioningsnappy> </a>
<heading2>SnappyData Provisioning Options</heading2>


<heading3>Prerequisites</heading3>

Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the *JAVA_HOME* environment variable is set on each computer.

The following options are available for provisioning SnappyData:

* [On-Premise](install/install_on_premise.md) <a id="install-on-premise"></a>

* [Amazon Web Services (AWS)](install/setting_up_cluster_on_amazon_web_services.md) <a id="setting-up-cluster-on-amazon-web-services-aws"></a>

* [Kubernetes](kubernetes.md)

* [Docker](/quickstart/getting_started_with_docker_image.md)

* [Building from Source](install/building_from_source.md)<a id="building-from-source"></a>

<heading3>Configuring the Limit for Open Files and Threads/Processes</heading3>

On a Linux system, you can set the limit of open files and thread processes in the **/etc/security/limits.conf** file. 
</br>A minimum of **8192** is recommended for open file descriptors limit and **>128K** is recommended for the number of active threads. 
</br>A typical configuration used for SnappyData servers and leads can appear as follows:

```pre
snappydata          hard    nofile      32768
snappydata          soft    nofile      32768
snappydata          hard    nproc       unlimited
snappydata          soft    nproc       524288
snappydata          hard    sigpending  unlimited
snappydata          soft    sigpending  524288
```
* `snappydata` is the user running SnappyData.

Recent linux distributions using systemd (like RHEL/CentOS 7, Ubuntu 18.04) require the **NOFILE** limit to be increased in systemd configuration too. Edit **/etc/systemd/system.conf ** as root, search for **#DefaultLimitNOFILE** under the **[Manager] **section. Uncomment and change it to **DefaultLimitNOFILE=32768**. 
Reboot for the above changes to be applied. Confirm that the new limits have been applied in a terminal/ssh window with **"ulimit -a -S"** (soft limits) and **"ulimit -a -H"** (hard limits).

