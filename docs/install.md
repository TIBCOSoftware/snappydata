# Provisioning SnappyData

SnappyData offers two editions of the product, SnappyData Community Edition, and SnappyData Enterprise Edition.

The SnappyData Community Edition is Apache 2.0 licensed. It is a free, open-source version of the product that can be downloaded by anyone.
The Enterprise Edition includes several additional capabilities that are closed source and only available as part of a licensed subscription.

For more information on the capabilities of the Community and Enterprise editions see [Community Edition (Open Source)/Enterprise Edition Components](additional_files/open_source_components.md).

<heading2> Download SnappyData Community Edition</heading2>

[Download the SnappyData 1.0.2 Community Edition (Open Source)](https://github.com/SnappyDataInc/snappydata/releases/) from the release page, which lists the latest and previous releases of SnappyData. The packages are available in compressed files (.tar format).

* [**SnappyData 1.0.2 Release download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.2/snappydata-1.0.2-bin.tar.gz)

* [**SnappyData 1.0.2 Release (user-provided Hadoop) download link**](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.2/snappydata-1.0.2-without-hadoop-bin.tar.gz) 

<heading2> Download SnappyData Enterprise Edition</heading2>

1. Go to the [SnappyData website](http://www.snappydata.io/download).

2. On this page, enter your email address.

3. Read the END USER LICENSE AGREEMENT and click the **Agree to terms of service** option to accept it.

4. Click **Download** to download the installer (**snappydata-1.0.2-bin.tar.gz**).

5. You can also download the following additional files by clicking on the links:

	* DEBIAN INSTALLER

	* REDHAT INSTALLER

	* JDBC JAR FILE

	* ODBC INSTALLERS

<heading2>Installation Options</heading2>

<heading3>Prerequisites</heading3>

Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the *JAVA_HOME* environment variable is set on each computer.

The following options are available for provisioning SnappyData:

* [On-Premise](install/install_on_premise.md) <a id="install-on-premise"></a>

* [Amazon Web Services (AWS)](install/setting_up_cluster_on_amazon_web_services.md) <a id="setting-up-cluster-on-amazon-web-services-aws"></a>

* [Kubernetes](kubernetes.md)

* [Docker](/quickstart/getting_started_with_docker_image.md)

* [Building from Source](install/building_from_source.md)<a id="building-from-source"></a>

**Note**:</br>
	**Configuring the limit for open files and threads/processes** </br> 
    On a Linux system, you can set the limit of open files and thread processes in the **/etc/security/limits.conf** file. 
    </br>A minimum of **8192** is recommended for open file descriptors limit and **>128K** is recommended for the number of active threads. 
    </br>A typical configuration used for SnappyData servers and leads can look like:

```pre
snappydata          hard    nofile      81920
snappydata          soft    nofile      8192
snappydata          hard    nproc       unlimited
snappydata          soft    nproc       524288
snappydata          hard    sigpending  unlimited
snappydata          soft    sigpending  524288
```

Here `snappydata` is the username under which the SnappyData processes are started.
