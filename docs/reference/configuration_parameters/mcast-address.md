# mcast-address

## Description


!!!Note 
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. </p>

Address used to discover other members of the distributed system. Only used if mcast-port is non-zero. This attribute must be consistent across the distributed system.


!!!Note 
	Select different multicast addresses and different ports for different distributed systems. Do not just use different addresses. Some operating systems may not keep communication separate between systems that use unique addresses but the same port number. </p>

This default multicast address was assigned by [IANA](http://www.iana.org/assignments/multicast-addresses). Consult the IANA chart when selecting another multicast address to use with SnappyData.

## Default Value

239.192.81.1

## Property Type

connection (boot)

## Prefix

gemfire.
