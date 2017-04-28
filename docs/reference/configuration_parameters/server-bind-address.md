# server-bind-address

## Description


!!!Note 
	This setting is only relevant for servers on multi-homed hosts (machines with multiple network cards). </p>

Network adapter card a SnappyData server binds to for client/server communication. You can use this to separate the server's client/server communication from its peer-to-peer communication, spreading the traffic load.

Specify the IP address, not the hostname, because each network card may not have a unique hostname.

This is a machine-wide attribute used for communication with clients in client/server and multi-site installations. It has no effect on locator location.

An empty string (the default) causes the servers to listen on the same card that is used for peer-to-peer communicaÂ­tion. This is either the bind-address or, if that is not set, the machine's default card.

## Default Value

not set

## Property Type

connection (boot)

## Prefix

gemfire.
