## Logging 

Currently, log files for SnappyData components go inside the working directory. To change the log file directory, you can specify a property _-log-file_ as the path of the directory. There is a log4j.properties file that is shipped with the jar. We recommend not to change it at this moment. However, to change the logging levels, you can create a conf/log4j.properties with the following details: 

```bash
$ cat conf/log4j.properties 
log4j.rootCategory=DEBUG, file
```