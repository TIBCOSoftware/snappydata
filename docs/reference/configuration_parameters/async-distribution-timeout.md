# async-distribution-timeout

## Description

!!! Note
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. </p>

Number of milliseconds a process that is publishing to this process should attempt to distribute a cache operation before switching over to asynchronous messaging for this process. The switch to asynchronous messaging lasts until this process catches up, departs, or some specified limit is reached, such as async-queue-timeout or async-max-queue-size.

To enable asynchronous messaging, the value must be set above zero. Valid values are in the range 0..60000.

## Default Value

0 (disabled)

## Property Type

connection (boot)

## Prefix

gemfire.
