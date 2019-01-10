# Log Message Format


Each message in a server or locator log file contains the severity level, timestamp, and other important information.

<a id="log-message"></a>
A SnappyData log message contains:

-   Severity level of the message.
-   Time that the message was logged.
-   ID of the thread that logged the message.
-   Body of the log message, which can be a string and/or an exception that includes the exception stack trace.

The following shows an example log entry.

```pre
[config 2011/05/24 17:19:45.705 IST <main> tid=0x1] This VM is setup with SnappyData datastore role.
```


