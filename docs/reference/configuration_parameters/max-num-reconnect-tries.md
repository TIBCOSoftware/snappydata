# max-num-reconnect-tries

## Description

The maximum number of reconnection attempts to make after the member is forcibly disconnected from the distributed system. 

See also <mark><a href="#jdbc_connection_attributes__section_7E1E441D83344259A3916399663E30C8" class="xref">max-wait-time-reconnect</a>, <a href="#jdbc_connection_attributes__disableautoreconnect" class="xref noPageCitation">disable-auto-reconnect</a>, and <a href="../../developers_guide/topics/server-side/fabricservice-reconnect.md#concept_22EE6DDE677F4E8CAF5786E17B4183A9" class="xref" title="A SnappyData member may be forcibly disconnected from a distributed system if it is unresponsive for a period of time, or if a network partition separates one or more members into a group that is too small to act as the distributed system. If you start SnappyData using the FabricService interface, you can use callback methods to perform actions during the reconnect process, or to cancel the reconnect process if necessary.">Handling Forced Disconnection</a>.</mark>

## Default Value

3

## Property Type

connection (boot)

## Prefix

gemfire.
