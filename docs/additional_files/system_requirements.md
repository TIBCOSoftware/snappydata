# System Requirements

In this section we discuss the hardware, software, and network requirements for SnappyData.

## Hardware  Requirements

SnappyData turns Apache Spark into a mission critical, elastic scalable in-memory data store. This allows users to run Spark workloads and classic database workloads on SnappyData.

**Memory**: SnappyData can run well with anywhere from 8GB of memory to hundreds of GB of memory. While exact memory requirements depend on the end user application, we recommend allocating no more than 75% of the memory to SnappyData. We recommend using a machine with at least 8GB of RAM when working with SnappyData

**CPU Cores**: SnappyData is a highly multi-threaded system and is able to take advantage of CPU cores to deliver higher throughput. It has been tested with multi-core multi-CPU machines. We recommend using machines with at least 16 cores when working with SnappyData. The degree of parallelism you can achieve with SnappyData directly depends on the number of cores and higher core machines will perform better than lower core machines.

**Network**: SnappyData is a clustered scale out in memory data store and both jobs and queries use the network extensively to complete their job. Since data is mostly available in memory, queries and jobs typically get CPU and/or network bound. We recommend running SnappyData on at least a 1GB network for testing and use a 10GB network for production scenarios.

**Disk**: SnappyData overflows data to local disk files and tables can be configured for persistence. We recommend using flash storage for optimal performance for SnappyData shared nothing persistence. Data can be saved out to stores like HDFS and S3 using SnappyData DataFrame APIs


## Operating Systems Supported

| Operating System| Version |
|--------|--------|
|Red Hat Enterprise Linux|        |
|Ubuntu|        |
|CentOS|        |


## Host Machine Requirements
Requirements for each host:

* A supported [Java SE installation](http://www.oracle.com/technetwork/java/javase/downloads/jdk-6u26-download-400750.html).

* File system that supports long file names.

* Adequate per-user quota of file handles (ulimit for Linux). Ensure that you set a high enough ulimit (up to the hard limit of 81,920) for your system.

* TCP/IP.

* System clock set to the correct time.

* For each Linux host, the hostname and host files must be properly configured. See the system manpages for hostname and hosts.

* For each Linux host, configure the swap volume to be the same size as the physical RAM installed in the computer.

* Time synchronization service such as Network Time Protocol (NTP).

!!! Note: 
	* For troubleshooting, you must run a time synchronization service on all hosts. Synchronized time stamps allow you to merge log messages from different hosts, for an accurate chronological history of a distributed run.
	* If you deploy RowStore to a virtualized host, see also [Running RowStore in Virtualized Environments](http://rowstore.docs.snappydata.io/docs/manage_guide/tuning-vm.html#concept_20E2D15D2F65442CAA0B04E9EA144F1D).

## Increase Unicast Buffer Size on Linux Platforms
On Linux platforms, execute the following commands as the root user to increase the unicast buffer size:

1. Edit the /etc/sysctl.conf file to include the following lines:</br>
 `net.core.rmem_max=1048576` </br>`net.core.wmem_max=1048576`

3. Reload sysctl.conf:</br>
 `sysctl -p`


## Disable SYN Cookies on Linux Platforms
Many default Linux installations use SYN cookies to protect the system against malicious attacks that flood TCP SYN packets. The use of SYN cookies dramatically reduces network bandwidth, and can be triggered by a running SnappyData cluster and its legitimate use of the network.

If your SnappyData cluster is otherwise protected against such attacks, disable SYN cookies to ensure that RowStore network throughput is not affected.

To disable SYN cookies permanently:

1. Edit the /etc/sysctl.conf file to include the following line: </br>
 `net.ipv4.tcp_syncookies = 0`

2. Setting this value to zero disables SYN cookies.

3. Reload sysctl.conf:</br>
 `sysctl -p`

## Filesystem Type for Linux Platforms
For optimum disk-store performance, we recommend the use of ext4 filesystems when operating on Linux or Solaris platforms. See [Optimizing a System with Disk Stores](http://rowstore.docs.snappydata.io/docs/disk_storage/running_system_with_disk_stores.html#running_system_with_disk_stores). (Shyja, this link is useful, and we need the contents from this, but we should check with Neeraj and Sumedh whether the settings referred to here are still the right ones)




