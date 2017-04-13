#Supported Configurations and System Requirements
This topic describes the supported configurations and system requirements for SnappyData.

Before you start the installation, make sure your system meets the minimum system requirements for installing and running the product.

The following topics are covered in this section:

* [Supported Configurations](#supported_config)
* [Host Machine Requirements](#host-machine)
* [Increase Unicast Buffer Size on Linux Platforms](#increase_buffer)
* [Disable SYN Cookies on Linux Platforms](#syn-cookies)
* [Filesystem Type for Linux Platforms](#filesystem-types)
* [SnappyData Dashboard Requirements](#dashboard)


<a id="supported-config"></a>
##Supported Configurations
The following table shows all supported configurations for SnappyData: 

<mark>Table- To be updated</mark>

| Operating System |Processor Architecture | JVM| Production or Developer Support
|--------|--------|--------|--------|
|Red Hat EL </br>5, 6.2, 6.4|x86 (64bit)|Java 8 |Production |
|CentOS </br>6.2, 6.4, 7-|x86 (64bit)|Java 8 |Production|
|Ubuntu 14.04 |x86 (64bit and 32 bit) |Java 8|Production |
|SUSE Linux Enterprise Server 11 |(64bit and 32 bit) |Java 8|Production |
|Ubuntu 10.11 |x86 (64bit) | Java 8|Production |

!!! Note:
	SnappyData download does not include Java; you must download and install a supported [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) for your system.

<a id="host-machine"></a>
##Host Machine Requirements
Requirements for each host:

* A supported [Java SE](http://www.oracle.com/technetwork/java/javase/overview/index.html) installation.

* File system that supports long file names.

* Adequate per-user quota of file handles (ulimit for Linux). Ensure that you set a high enough ulimit (up to the hard limit of 81,920) for your system.

* TCP/IP.

* System clock set to the correct time.

* For each Linux host, the hostname and host files must be properly configured. See the system manpages for hostname and hosts. <mark>Add link or details</mark>

* For each Linux host, configure the swap volume to be the same size as the physical RAM installed in the computer.

* Time synchronization service such as Network Time Protocol (NTP).\

!!! Note:
	* For troubleshooting, you must run a time synchronization service on all hosts. Synchronized time stamps allow you to merge log messages from different hosts, for an accurate chronological history of a distributed run.



<a id="increase_buffer"></a>
##Increase Unicast Buffer Size on Linux Platforms
On Linux platforms, execute the following commands as the root user to increase the unicast buffer size:

1. Edit the /etc/sysctl.conf file to include the following lines:</br>
 ```
 net.core.rmem_max=1048576
 net.core.wmem_max=1048576
 ```

2. Reload sysctl.conf:</br>
 ```
 sysctl -p
 ```

<a id="syn-cookies"></a>
##Disable SYN Cookies on Linux Platforms
Many default Linux installations use SYN cookies to protect the system against malicious attacks that flood TCP SYN packets. The use of SYN cookies dramatically reduces network bandwidth, and can be triggered by a running SnappyData distributed system.

If your SnappyData distributed system is otherwise protected against such attacks, disable SYN cookies to ensure that SnappyData network throughput is not affected.

To disable SYN cookies permanently:

1. Edit the /etc/sysctl.conf file to include the following line: </br>
 ```
 net.ipv4.tcp_syncookies = 0
 ```
 </br>Setting this value to zero disables SYN cookies.

2. Reload sysctl.conf: </br>
 ```
 sysctl -p
 ```
 

<a id="filesystem-types"></a>
##Filesystem Type for Linux Platforms
For optimum disk-store performance, SnappyData recommends the use of ext4 filesystems when operating on Linux or Solaris platforms. See Optimizing a System with Disk Stores <mark> Add Link</mark>

<a id="dashboard"></a>
## SnappyData Dashboard Requirements
The SnappyData Dashboard has been tested for compatibility with the following Web browsers. 

* Internet Explorer 11.0.37
* Safari 5.1.7 for Windows
* Google Chrome 53.0.2785.89
* Mozilla Firefox 48.0