# redundancy-zone

## Description

Defines this member's redundancy zone. Used to separate members into different groups for satisfying partitioned table redundancy. If this property is set, SnappyData does not put redundant copies of data in members with the same redundancy zone setting.

For example, if you had redundancy set to 1, so you have one primary and one secondary copy of each data entry, you could split primary and secondary data copies between two machine racks by defining one redundancy zone for each rack. 

You set one redundancy zone in the *conf/servers* file for all members that run on one rack:

```pre
-gemfire.redundancy-zone=rack1
```

You can also set another redundancy zone in the *conf/servers* file for all members that run on another rack:

```pre
-gemfire.redundancy-zone=rack2
```

## Default Value

not set

## Property Type

connection (boot)

## Prefix

gemfire.

## Example

```pre
localhost1 -gemfire.redundancy-zone=rack1
localhost1 -gemfire.redundancy-zone=rack1
localhost2 -gemfire.redundancy-zone=rack2
localhost2 -gemfire.redundancy-zone=rack2
```
