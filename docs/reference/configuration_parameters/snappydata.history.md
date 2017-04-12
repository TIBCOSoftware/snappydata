# snappydata.history


## Description

The path and filename in which the `snappy-shell` utility stores a list of the commands executed during an interactive `snappy-shell` session. Specify this system property in the JAVA\_ARGS environment variable before you execute `snappy-shell` (for example, JAVA\_ARGS="-Dgfxd.history=*path-to-file*"). Specify an empty string value to disable recording a history of commands. See [snappy-shell Interactive Commands](../interactive_commands/store_command_reference.md).

## Default Value

<span class="ph filepath">%UserProfile%\\.gfxd.history</span> (Windows)

<span class="ph filepath">$HOME/.gfxd.history</span> (Linux)

## Property Type

system

## Prefix

n/a
