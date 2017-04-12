# async-queue-timeout

## Description

!!!Note 
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. It affects asynchronous queues for members that publish to this member. </p>

Maximum milliseconds the publisher waits with no distribution to this member before it asks the member to leave the distributed system. Used for handling slow receivers.

## Default Value

6000

## Property Type

connection (boot)

## Prefix

gemfire.
