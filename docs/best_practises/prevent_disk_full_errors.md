# Preventing Disk Full Errors

It is important to monitor the disk usage of SnappyData members. If a member lacks sufficient disk space for a disk store, the member attempts to shut down the disk store and its associated tables, and logs an error message. After you make sufficient disk space available to the member, you can restart the member. 
<!--(See <a href="#concept_83A53C7C767442578F2A4CF50E4A224E__memberstartupddreplay" class="xref">Member Startup Problems</a>.)
-->
A shutdown due to a member running out of disk space can cause loss of data, data file corruption, log file corruption and other error conditions that can negatively impact your applications.

You can prevent disk file errors using the following techniques:

* Use default pre-allocation for disk store files and disk store metadata files. Pre-allocation reserves disk space for these files and leaves the member in a healthy state when the disk store is shut down, allowing you to restart the member once sufficient disk space has been made available. Pre-allocation is configured by default.
</br>Pre-allocation is governed by the following system properties:

	- **Disk store files**: Set the <a href="reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes__preallocatedisk" class="xref">gemfire.preAllocateDisk</a> system property to true (the default).

	- **Disk store metadata files**: Set the <a href="reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes__preallocateif" class="xref">gemfire.preAllocateIF</a> system property to true (the default).

	!!! Note
 		It is recommended to use ext4 filesystems on Linux platforms, because ext4 supports preallocation which speeds disk startup performance. If you are using ext3 filesystems in latency-sensitive environments with high write throughput, you can improve disk startup performance by setting the the MAXLOGSIZE property of a disk store to a value lower than the default 1 GB. See [CREATE DISKSTORE](#).</p>

* Monitor SnappyData logs for low disk space warnings. SnappyData logs disk space warnings in the following situations:

    -   **Log file directory**: Logs a warning if the available space is less than 100 MB.
    -   **Disk store directory**: Logs a warning if the usable space is less than 1.15 times the space required to create a new oplog file.
    -   **Data dictionary**: Logs a warning if the remaining space is less than 50 MB.

    You can configure the log message frequency with the <a href="reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes__diskspace-warning-interval" class="xref">gemfire.DISKSPACE\_WARNING\_INTERVAL</a> system property.
