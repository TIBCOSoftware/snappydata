# disable-auto-reconnect

## Description

Specifies whether the SnappyData member should automatically reconnect to the distributed system after a forced disconnect. A SnappyData member may be forcibly disconnected from a distributed system if it is unresponsive for a period of time, or if a network partition separates one or more members into a group that is too small to act as the distributed system. When this property is set to "false" (the default), the member automatically shuts down and then restarts into a "reconnecting" state, where it periodically attempts to rejoin the distributed system. If it succeeds in reconnecting, the member rebuilds its view of the distributed system from existing members, and it receives a new distributed system ID. 

See also [max-num-reconnect-tries.md](max-num-reconnect-tries.md), [max-wait-time-reconnect](max-wait-time-reconnect.md), and <mark>Handling Forced Disconnection](http://rowstore.docs.snappydata.io/docs/developers_guide/topics/server-side/fabricservice-reconnect.html#concept_22EE6DDE677F4E8CAF5786E17B4183A9). To be confirmed</mark>

## Default Value

false

## Property Type

connection (boot)

## Prefix

gemfire.
