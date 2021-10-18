# snappy.history

## Description

The path and filename in which the `snappy` utility stores a list of the commands executed during an interactive `snappy` session. Specify this system property in the JAVA_ARGS environment variable before you execute `snappy` (for example, JAVA_ARGS="-Dsnappy.history=*path-to-file*"). Specify an empty string value to disable recording a history of commands. See [Snappy-SQL Shell Interactive Commands](../interactive_commands/index.md).

## Default Value

%UserProfile%\\.snappy.history (Windows)

$HOME/.snappy.history (Linux)

## Property Type

system

## Prefix

n/a

## Example

```
export JAVA_ARGS="-Dsnappy.history=*path-to-file*"
```
