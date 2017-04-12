# ack-wait-threshold

## Description

The number of seconds a distributed message waits for acknowledgment before it sends an alert to signal that something might be wrong with the system member that is unresponsive. After sending this alert the waiter continues to wait. The alerts are logged in the system members log as warnings.

Valid values are in the range 0...2147483647

## Default Value

15

## Property Type

connection (boot)

## Prefix

gemfire.
