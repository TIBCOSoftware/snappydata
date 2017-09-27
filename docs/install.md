# Download and Install

SnappyData offers two editions of the product, they are the Community edition and the Enterprise edition.</br>
The Community edition that is Apache V2 licensed, is a free, open-source version of the product which anyone can download.</br>
The Enterprise edition of the product includes everything that is offered in the Community edition and includes additional capabilities that are closed source and only available as part of a licensed subscription. 

For more information on the capabilities of the Community and Enterprise edition see, [Community Edition/Enterprise Edition Components](additional_files/open_source_components.md).

## Download SnappyData Community Edition

* [Download SnappyData 1.0 Community Edition](https://github.com/SnappyDataInc/snappydata/releases/):</br>
 This page lists the latest and previous releases of SnappyData. The packages are available in compressed files (.zip and .tar format). 

	* **SnappyData 1.0.0 Release download link**
[(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.0/snappydata-1.0.0-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.0/snappydata-1.0.0-bin.zip)

	* **SnappyData 1.0.0 Release (user-provided Hadoop) download link** [(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.0/snappydata-1.0.0-without-hadoop-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v1.0.0/snappydata-1.0.0-without-hadoop-bin.zip)

## Download SnappyData Enterprise Edition

1. Go to the [SnappyData website](http://www.snappydata.io/download).

2. On this page, enter your email address.

3. Read the END USER LICENSE AGREEMENT and click the **Agree to the terms of service** option to accept it.

4. Click **Download** to download the installer (**snappydata-enterprise-1.0.0-rc1.tar.gz**).

5. You can download the following additional files by clicking on the provided links.

	* DEBIAN INSTALLER

	* REDHAT INSTALLER

	* JDBC JAR FILE

	* ODBC INSTALLERS

## Installation Options

### Prerequisites
Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the *JAVA_HOME* environment variable is set on each computer.

The following installation options are available:

* [Install On-Premise](install/install_on_premise.md)

* [Setting up Cluster on Amazon Web Services (AWS)](install/setting_up_cluster_on_amazon_web_services.md)

* [Building from Source](install/building_from_source.md)

!!! Note:  
	**Configuring the limit for open files and threads/processes** </br> 
    On a Linux system, you can set the limit of open files and thread processes in the **/etc/security/limits.conf** file. 
    </br>A minimum of **8192** is recommended for open file descriptors limit and **>128K** is recommended for the number of active threads. 
    </br>A typical configuration used for SnappyData servers and leads can look like:

```
snappydata          hard    nofile      81920
snappydata          soft    nofile      8192
snappydata          hard    nproc       unlimited
snappydata          soft    nproc       524288
snappydata          hard    sigpending  unlimited
snappydata          soft    sigpending  524288
```

Here `snappydata` is the user name under which the SnappyData processes are started.