# socket-lease-time

## Description

The time, in milliseconds, that a thread can have exclusive access to a socket it is not actively using. A value of zero causes socket leases to never expire. This property is ignored if conserve-sockets is true.

Valid values are in the range 0..600000.

## Default Value

60000

## Property Type

connection (boot)

## Prefix

gemfire.
