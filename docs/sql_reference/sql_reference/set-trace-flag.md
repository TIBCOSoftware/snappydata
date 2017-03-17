# SYS.SET_TRACE_FLAG

Enables a `snappydata.debug.true` trace flag on all members of the distributed system, including locators.

This procedure enables or disables a specific trace flag for the distributed system as a whole. You must be a system user to execute this procedure. See <a href="../../manage_guide/log-debug.html#concept_0F36D4EF575C42069159670739D1ECC8" class="xref" title="RowStore provides debug trace flags to record additional information about RowStore features in the log file.">Using Trace Flags for Advanced Debugging</a> for a description of some commonly-used trace flags.

##Syntax

``` pre
SYS.SET_TRACE_FLAG (
IN TRACE_FLAG VARCHAR(256),
IN ON BOOLEAN
)
```

TRACE\_FLAG   
Specifies name of the trace flag to enable or disable.

<!-- -->

ON   
Specifies boolean value: "true" to enable the trace flag, or "false" to disable it.

##Example

This command traces all JAR installation, update, and removal operations in the RowStore distributed system:

``` pre
call sys.set_trace_flag ('TraceJars', 'true');
```


