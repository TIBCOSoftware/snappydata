# SYS.DUMP_STACKS

Writes thread stacks, locks, and transaction states to the TIBCO ComputeDB log file. You can write stack information either for the current TIBCO ComputeDB member or for all TIBCO ComputeDB members in the distributed system.

<!--See also [print-stacks](../command_line_utilities/store-print-stacks.md) for information about writing thread stacks to standard out or to a specified file.--->

## Syntax

```pre
SYS.DUMP_STACKS (
IN ALL BOOLEAN
)
```

**ALL**   
Specifies boolean value: **true** to log stack trace information for all TIBCO ComputeDB members, or **false** to log information only for the local TIBCO ComputeDB member.

## Example

This command writes thread stack information only for the local TIBCO ComputeDB member. The stack information is written to the TIBCO ComputeDB log file (by default snappyserver.log in the member startup directory):

```pre
snappy> call sys.dump_stacks('false');
```

