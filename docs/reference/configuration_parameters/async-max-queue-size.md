# async-max-queue-size

## Description

!!!Note:
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. It applies to non-conflated asynchronous queues for members that publish to this member. </p> Maximum size of the queue in megabytes before the publisher asks this member to leave the distributed system.

Valid values are in the range 0..1024.

## Default Value

8

## Property Type

connection (boot)

## Prefix

gemfire.
