# SYS.SET_TRACE_FLAG

Enables a `snappydata.debug.true` trace flag on all members of the distributed system, including locators.

This procedure enables or disables a specific trace flag for the distributed system as a whole. You must be a system user to execute this procedure. <!-- See <mark> TO BE CONFIRMED RowStore Link [Using Trace Flags for Advanced Debugging](http://rowstore.docs.snappydata.io/docs/manage_guide/log-debug.html)</mark> for a description of some commonly-used trace flags.-->

##Syntax

``` pre
SYS.SET_TRACE_FLAG (
IN TRACE_FLAG VARCHAR(256),
IN ON BOOLEAN
)
```

TRACE_FLAG   
Specifies name of the trace flag to enable or disable.

ON   
Specifies boolean value: "true" to enable the trace flag, or "false" to disable it.

##Example

This command traces all JAR installation, update, and removal operations in the SnappyData distributed system:

``` pre
call sys.set_trace_flag ('TraceJars', 'true');
```


