# gemfire.DISKSPACE_WARNING_INTERVAL

## Description

Configures the frequency (in milliseconds) with which RowStore logs warning messages for low disk space conditions. RowStore logs a low disk space warning in the following situations:

-   For a log file directory it logs a warning if the available space is less than 100 MB.
-   For a disk store directory it logs a warning if the usable space is less than 1.15 times the space required to create a new oplog file.
-   For the data dictionary directory it logs a warning if the remaining space is less than 50 MB.

## Default Value

10000

## Property Type

system

## Prefix

n/a
