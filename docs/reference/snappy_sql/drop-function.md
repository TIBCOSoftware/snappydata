#Drop Function

```
DROP [TEMPORARY] FUNCTION [IF EXISTS] [db_name.]function_name
```

Drop an existing function. If the function to drop does not exist, an exception will be thrown.

!!! Note: 
	This command is supported only when Hive support is enabled.

**TEMPORARY**
Whether to function to drop is a temporary function.

**IF EXISTS**
If the function to drop does not exist, nothing will happen