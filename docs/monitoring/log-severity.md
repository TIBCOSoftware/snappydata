# Severity Levels


You can configure the logging system to record only those messages that are at or above a specified logging level. By default, the logging level is set to "config", which means that the system logs messages at config, info, warning, error, and severe severity levels.

<a id="log-level"></a>
If you are having problems with your system, first lower the log-level (recording more of detailed messages to the log file) and recreate the problem. The additional log messages often help uncover the source.

To specify the logging level, use the `log-level` property when you start a SnappyData server or locator. For example, to record all messages at log level "warning," "error," or "severe:"

```pre
snappy-shell snappydata server start -log-file=/home/user1/log/mygfxdlog.log -log-level=warning
```

The table shows log levels from highest to lowest severity.

| Log Level              | Indication                                                                                                                                                                                                                                                                                                                                                                                            |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| severe                 | Highest severity level, indicating a serious failure that usually prevents normal program execution. You may need to shut down or restart part of your cluster to correct the problem. |
| error                  | The operation indicated in the error message has failed. The server or locator should continue to run.      |
| warning                | Potential problem with the system. In general, warning messages describe events that are of interest to end users or system managers.                                                                                                                                                                                                                                                              |
| info                   | Informational messages for end users and system administrators.                                                                                                                                                                                                                                                                                                                                       |
| config                 | Default severity level for logging messages. This log level provides static configuration messages that you can use to debug configuration problems.                                                                                                                                                                                                                                                  |
| fine                   | Tracing information that is generally of interest only to application developers. This logging level may generate lots of "noise" that might not indicate a problem in your application. It creates very verbose logs that may require significantly more disk space than logs that record only higher severity levels. Do not use this log setting unless you are asked to do so by Pivotal Support. |
| finer, finest, and all | Reserved for internal use. These log levels produce a large amount of data and consume large amounts of disk space and system resources. Do not use these settings unless you are asked to do so by Pivotal Support.                                                                                                                                                                                  |


