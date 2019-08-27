# SYS.SET_TRACE_FLAG

This procedure enables or disables a specific trace flag for the distributed system as a whole. You must be a system user to execute this procedure.

## Syntax

```pre
CALL SYS.SET_TRACE_FLAG (
IN TRACE_FLAG VARCHAR(256),
IN ON BOOLEAN
)
```

TRACE_FLAG   
Specifies name of the trace flag to enable or disable.

ON   
Specifies boolean value: **true** or **1** to enable the trace flag, or **false** or **0** to disable it.

## Example

This command traces all JAR installation, update, and removal operations in the SnappyData distributed system:

```pre
snappy> call sys.set_trace_flag ('TraceJars', 'true');
```

**Also see:**

*	[Built-in System Procedures and Built-in Functions](system-procedures.md)
