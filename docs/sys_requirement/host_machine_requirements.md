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

