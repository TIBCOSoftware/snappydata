# max-wait-time-reconnect

## Description

The maximum time in milliseconds to wait for the distributed system to reconnect between each reconnection attempt. 

<mark>See also [max-num-reconnect-tries](max-num-reconnect-tries.md), [disable-auto-reconnect](disable-auto-reconnect.md), and <a href="../../developers_guide/topics/server-side/fabricservice-reconnect.md#concept_22EE6DDE677F4E8CAF5786E17B4183A9" class="xref" title="A SnappyData member may be forcibly disconnected from a distributed system if it is unresponsive for a period of time, or if a network partition separates one or more members into a group that is too small to act as the distributed system. If you start SnappyData using the FabricService interface, you can use callback methods to perform actions during the reconnect process, or to cancel the reconnect process if necessary.">Handling Forced Disconnection</a>.
</mark>

## Default Value

60000

## Property Type

connection (boot)

## Prefix

gemfire.
