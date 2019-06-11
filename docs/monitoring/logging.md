# Configuring and Using SnappyData Log Files


By default SnappyData creates a log file named <span class="ph filepath">gfxdserver.log</span> in the current directory when you start a server programmatically or using the `snappy-shell` utility. SnappyData creates a log file named <span class="ph filepath">gfxdlocator.log</span> in the current working directory when you start a locator.

You can specify the name and location of the log file by using the JDBC boot property `log-file` when you start a server or locator. For example:

```pre
snappy-shell snappydata server start -log-file=/home/user1/log/mysnappystorelog.log
```

-   **[Product Usage Logging](../../manage_guide/Topics/membership-logging.html)**
    Each SnappyData locator creates a log file that record the different membership views of the SnappyData distributed system. You can use the contents of these log files to verify that your product usage is compliance with your SnappyData license.

-   **[Log Message Format](../../manage_guide/log-format.html)**

    Each message in a server or locator log file contains the severity level, timestamp, and other important information.
-   **[Severity Levels](../../manage_guide/log-severity.html)**

    You can configure the logging system to record only those messages that are at or above a specified logging level. By default, the logging level is set to "config", which means that the system logs messages at config, info, warning, error, and severe severity levels.

-   **[Using java.util.logging.Logger for Application Log Messages](../../manage_guide/log-application.html)**
    Applications that use the SnappyData JDBC peer driver can log messages through the java.util.Logger logging API. You can obtain a handle to the java.util.Logger logger object by entering `com.pivotal.gemfirexd` after the application connects to the SnappyData cluster with the peer driver.

-   **[Using Trace Flags for Advanced Debugging](../../manage_guide/log-debug.html)**
    SnappyData provides debug trace flags to record additional information about SnappyData features in the log file.


