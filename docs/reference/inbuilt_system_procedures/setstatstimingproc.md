# SYSCS_UTIL.SET_STATISTICS_TIMING

When statistics timing is turned on, you can track the timing of various aspects of a statement execution. When statistics timing is turned off, all timing values are set to zero.

Statistics timing is an attribute associated with a connection that you turn on and off by using the `SYSCS_UTIL.SET_STATISTICS_TIMING` system procedure. Statistics timing is turned off by default. Turn statistics timing on only when the statistics are being collected with <mark>[SYSCS_UTIL.SET_EXPLAIN_CONNECTION] . If you turn statistics timing on before enabling statistics collection with SYSCS_UTIL.SET_EXPLAIN_CONNECTION, the procedure has no effect. To Be Confirmed</mark>

Turn statistics timing on by calling this procedure with a non-zero argument. Turn statistics timing off by calling the procedure with a zero argument.<mark> To be confirmed See Capture Query Plans for All Statements </mark>.

## Syntax

```pre
SYSCS_UTIL.SET_STATISTICS_TIMING(IN SMALLINT ENABLE)
```

##Example

To enable statistics timing:

```pre
CALL SYSCS_UTIL.SET_STATISTICS_TIMING(1);
```
