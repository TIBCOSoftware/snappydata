# System Requirements

In this section, we discuss the hardware, software, and network requirements for SnappyData.

## Hardware  Requirements

SnappyData turns Apache Spark into a mission-critical, elastic scalable in-memory data store. This allows users to run Spark workloads and classic database workloads on SnappyData.

**Memory**: SnappyData works well with anywhere from 8GB of memory to hundreds of GB of memory. While exact memory requirements depend on the end user application, we recommend allocating no more than 75% of the memory to SnappyData. We recommend using a machine with at least 8GB of RAM when working with SnappyData.

!!!Note
	It is recommended to have a minimum of 8GB memory for server-grade machines.
    
**CPU Cores**: SnappyData is a highly multi-threaded system and can take advantage of CPU cores to deliver higher throughput. It has been tested with multi-core multi-CPU machines. We recommend using machines with at least 16 cores when working with SnappyData. The degree of parallelism you can achieve with SnappyData directly depends on the number of cores, as higher core machines perform better than lower core machines.

**Network**: SnappyData is a clustered scale-out in-memory data store and both jobs and queries use the network extensively to complete their job. Since data is mostly available in-memory, queries and jobs typically get CPU and/or network bound. We recommend running SnappyData on at least a 1GB network for testing and use a 10GB network for production scenarios.

**Disk**: SnappyData overflows data to local disk files and tables can be configured for persistence. We recommend using flash storage for optimal performance for SnappyData shared nothing persistence. Data can be saved out to stores like HDFS and S3 using SnappyData DataFrame APIs.


## Operating Systems Supported

| Operating System| Version |
|--------|--------|
|Red Hat Enterprise Linux|- RHEL 6.0 </p> - RHEL 7.0 (Mininum recommended kernel version: 3.10.0-693.2.2.el7.x86_64)|
|Ubuntu|Ubuntu Server 14.04 and later||
|CentOS|CentOS 6, 7 (Minimum recommended kernel version: 3.10.0-693.2.2.el7.x86_64)|


## Host Machine Requirements
Requirements for each host:

* A supported [Oracle Java SE 8](http://www.oracle.com/technetwork/java/javase/downloads) installation. We recommend minimum version: 1.8.0_144 (see [SNAP-2017](https://jira.snappydata.io/browse/SNAP-2017), [SNAP-1999](https://jira.snappydata.io/browse/SNAP-1999), [SNAP-1911](https://jira.snappydata.io/browse/SNAP-1911), [SNAP-1375](https://jira.snappydata.io/browse/SNAP-1375) for crashes reported with earlier versions).

* The latest version of Bash shell.

* A file system that supports long file names.

* TCP/IP.

* System clock set to the correct time.

* For each Linux host, the hostname and host files must be properly configured. See the system manual pages for hostname and host settings.

* For each Linux host, configure the swap to be in the range of 32-64GB to allow for swapping out of unused pages.

* Time synchronization service such as Network Time Protocol (NTP).

* cURL must be installed on lead nodes for snappy scripts to work. On Red Hat based systems it can be installed using `sudo yum install curl` while on Debian/Ubuntu based systems, you can install using `sudo apt-get install curl `command.

!!! Note
	* For troubleshooting, you must run a time synchronization service on all hosts. Synchronized time stamps allow you to merge log messages from different hosts, for an accurate chronological history of a distributed run.

	* If you deploy SnappyData on a virtualized host, consult the documentation provided with the platform, for system requirements and recommended best practices, for running Java and latency-sensitive workloads.

## VSD Requirements
- Install 32-bit libraries on 64-bit Linux:</br>
	"yum install glibc.i686 libX11.i686" on RHEL/CentOS</br>
	"apt-get install libc6:i386 libx11-6:i386" on Ubuntu/Debian like systems</br>

- Locally running X server. For example, an X server implementation like, XQuartz for Mac OS, Xming for Windows OS, and Xorg which is installed by default for Linux systems.

## Python Integration using pyspark 
-	The Python pyspark module has the same requirements as in Apache Spark. The numpy package is required by many modules of pyspark including the examples shipped with SnappyData. On recent Red Hat based systems, it can be installed using `sudo yum install numpy` or `sudo yum install python2-numpy` commands. Whereas, on Debian/Ubuntu based systems, you can install using the `sudo apt-get install python-numpy` command.

-	Some of the python APIs can use SciPy to optimize some algorithms (in linalg package), and some others need Pandas. On recent Red Hat based systems SciPy can be installed using `sudo yum install scipy` command. Whereas,  on Debian/Ubuntu based systems you can install using the `sudo apt-get install python-scipy` command.. Likewise, Pandas on recent Red Hat based systems can be installed using `sudo yum installed python-pandas` command, while on Debian/Ubuntu based systems it can be installed using the `sudo apt-get install python-pandas` command.

-	On Red Hat based systems, some of the above Python packages may be available only after enabling the **EPEL** repository. If these are not available in the repositories for your OS version or if using **EPEL** is not an option, then you can use **pip**. Refer to the respective project documentation for details and alternative options such as Anaconda.

## Filesystem Type for Linux Platforms

For optimum disk-store performance, we recommend the use of local filesystem for disk data storage and not over NFS.


