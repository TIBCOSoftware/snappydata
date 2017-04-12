# mcast-port

## Description


!!!Note 
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. </p>

Port used, along with the mcast-address, for multicast communication with other members of the distributed system. If zero, multicast is disabled for member discovery and distribution.


!!!Note 
	Select different multicast addresses and ports for different distributed systems. Do not just use different addresses. Some operating systems may not keep communication separate between systems that use unique addresses but the same port number. </p>

Valid values are in the range 0..65535.

Either `mcast-port` or `locators` is **required** for a peer. If `mcast-port` is specified, do not specify `locators` or set `start-locator`.

## Default Value

10334

## Property Type

connection (boot)

## Prefix

gemfire.
