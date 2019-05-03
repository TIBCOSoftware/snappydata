# member-timeout

## Description

Interval, in milliseconds, between two attempts to determine whether another system member is alive. When another member appears to be gone, TIBCO ComputeDB tries to contact it twice before quitting.

Valid values are in the range 1000..600000.

## Default Value

5000

## Property Type

connection (boot)

## Prefix

gemfire.
