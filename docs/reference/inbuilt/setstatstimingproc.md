# SYSCS_UTIL.SET_STATISTICS_TIMING

When statistics timing is turned on, you can track the timing of various aspects of a statement execution. When statistics timing is turned off, all timing values are set to zero.

<a id="rrefsetstatstimingproc__section_993A648BF3914C5890C7330B7CF63DCD"></a>
Statistics timing is an attribute associated with a connection that you turn on and off by using the `SYSCS_UTIL.SET_STATISTICS_TIMING` system procedure. Statistics timing is turned off by default. Turn statistics timing on only when the statistics are being collected with <a href="set_explain_connection.html#rrefsetstatstimingproc" class="xref noPageCitation" title="Enables or disables capturing query execution plans and statistics for the current connection.">SYSCS\_UTIL.SET\_EXPLAIN\_CONNECTION</a>. If you turn statistics timing on before enabling statistics collection with SYSCS\_UTIL.SET\_EXPLAIN\_CONNECTION, the procedure has no effect.

<a id="rrefsetstatstimingproc__section_2FEFB71BBB8A4D539128E6C4628A561A"></a>

Turn statistics timing on by calling this procedure with a non-zero argument. Turn statistics timing off by calling the procedure with a zero argument. See <a href="../../../manage_guide/query-enabling.html#concept_172AA25A5DA945ED86DB66F5FA0E27C1" class="xref" title="As an alternative to using the EXPLAIN command, you can use built-in system procedures to enable and disable query execution plan and statistics capture for all statements that you execute on a connection.">Capture Query Plans for All Statements</a>.

<a id="rrefsetstatstimingproc__section_24C37D865D21497E8C9538F04BDB4F85"></a>

##Syntax

``` pre
SYSCS_UTIL.SET_STATISTICS_TIMING(IN SMALLINT ENABLE)
```

<a id="rrefsetstatstimingproc__section_0F2DFB96E0AC4AA78F2F74947F80E02B"></a>

##Example

To enable statistics timing:

``` pre
CALL SYSCS_UTIL.SET_STATISTICS_TIMING(1);
```
