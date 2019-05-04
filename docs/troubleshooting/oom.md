## Troubleshooting Out-of-Memory(OOM)Errors

Whenever TIBCO ComputeDB faces an Out-of-Memory(OOM) situation, the processes are killed. The pid files that are created in the TIBCO ComputeDB work directory are an indication that an OOM error has occured. Additional files can also be found based on the provided cluster configuration options. These files can be used for further analysis. 

You can use the following pointers to troubleshoot the OOM error:

*	**jvmkill_pid.log** (pid of process) file:
	This file can be found in the work directory which indicates that the system has faced an OOM error and has been killed. The pid can be found in the file name.
*	Following files can also be found depending on the additional configuration provided:
	*	**java_pid(pidof process)-(time when process killed).hprof(pid)** file is found if an additional configuration (**-XX:+HeapDumpOnOutOfMemoryError**) is given. For example, **java_pid1891-20190504_123015.hprof1891**. The **hprof** file contains the heapdump that is useful for analysis.
	*	In case the additional configuration is not provided, the **java_pid(pid of process)-(time when process killed).jmap** file can be found. This contains the map of heap for analysis.

In case there are any exceptions while writing the heap dump files, you will find a log entry of the respective process. For example, **Failed to log heap histogram for pid: (pid of process)**