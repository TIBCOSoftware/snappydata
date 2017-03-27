#Show Functions


```
SHOW [USER|SYSTEM|ALL] FUNCTIONS ([LIKE] regex | [db_name.]function_name)
```

Show functions matching the given regex or function name. If no regex or name is provided then all functions will be shown. IF `USER` or `SYSTEM` is declared then these will only show user-defined Spark SQL functions and system-defined Spark SQL functions respectively.

**LIKE**
This qualifier is allowed only for compatibility and has no effect.