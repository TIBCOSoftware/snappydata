# Evaluating Statistics for the System

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent> 

SnappyData provides statistics for analyzing system performance. Any member of a distributed system, including SnappyData servers, locators, and peer clients, can collect and archive this statistical data.

SnappyData samples statistics at a configurable interval and writes them to an archive. The archives can be read at any time, including at runtime.

You can view and analyze runtime or archived historical data using these tools:

-   `snappy-shell stats` is a command-line tool provided with the SnappyData product.

-   SnappyData [Visual Statistics Display (VSD)](vsd_overview.md) is a graphical tool that is installed in the **vsd** subdirectory of the SnappyData installation.

!!! Note
	- SnappyData statistics use the Java System.nanoTimer for nanosecond timing. This method provides nanosecond precision, but not necessarily nanosecond accuracy. For more information, see the online Java documentation for System.nanoTimer for the JRE you are using with SnappyData. 
	- Runtime viewing of statistics archives files is not necessarily real-time, because of file system buffering. </p>

**More Information**

-   **[Collecting System Statistics](collecting_system_stats.md)**
    Enable SnappyData system statistics using a system procedure, member boot properties, or connection properties.

-   **[Using VSD to Analyze Statistics](vsd_overview.md)**
    The Visual Statistics Display (VSD) reads the sampled statistics from one or more archives and produces graphical displays for analysis. VSD is installed with SnappyData in the tools subdirectory.


