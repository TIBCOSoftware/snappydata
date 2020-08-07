#  Using VSD to Analyze Statistics

The Visual Statistics Display (VSD) reads the sampled statistics from one or more archives and produces graphical displays for analysis. VSD is installed with TIBCO ComputeDB in the <span class="ph filepath">tools</span> subdirectory.

VSD’s extensive online help offers complete reference information about the tool. 

!!! Note
	Ensure the following for running VSD:

	- Install 32-bit libraries on 64-bit Linux:</br>
		"yum install glibc.i686 libX11.i686" on RHEL/CentOS</br>
		"apt-get install libc6:i386 libx11-6:i386" on Ubuntu/Debian like systems</br>

	- Locally running X server. For example, an X server implementation like, XQuartz for Mac OS, Xming for Windows OS, and Xorg which is installed by default for Linux systems.

**More information**

-   **[Installing and Running VSD](running_vsd.md)**

-   **[Transaction Performance](vsd_transactions.md)**

-   **[Table Performance](vsd_tables.md)**

-   **[SQL Statement Performance](vsd_statements.md)**

-   **[Memory Usage](vsd_memory.md)**

-   **[Client Connections](vsd-connection-stats.md)**

-   **[CPU Usage](vsd_cpu.md)**


