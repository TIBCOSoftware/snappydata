# sync-commits

## Description
Determines whether second-phase commit actions occur in the background for the current connection, or whether the connection waits for second-phase commit actions to complete. By default (sync-commits=false) TIBCO ComputeDB performs second-phase commits in the background, but ensures that the connection that issued the transaction only sees completed results. This means that other threads or connections may see different results until the second-phase commit actions complete.</br>
Using `sync-commits=true` ensures that the current thin client or peer client connection waits until all second-phase commit actions complete.

## Default Value
false

## Property Type
connection (boot)

## Prefix
n/a
