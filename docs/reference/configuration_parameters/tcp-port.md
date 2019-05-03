# tcp-port

## Description

TCP port to listen on for cache communications. If set to zero, the operating system selects an available port. Each process on a machine must have its own TCP port. Some operating systems restrict the range of ports usable by non-privileged users, and using restricted port numbers can cause runtime errors in TIBCO ComputeDB startup.

Valid values are in the range 0..65535.

## Default Value

0

## Property Type

connection (boot)

## Prefix

gemfire.
