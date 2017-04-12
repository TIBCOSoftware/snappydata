# disable-auto-reconnect

## Description

Specifies whether the RowStore member should automatically reconnect to the distributed system after a forced disconnect. A RowStore member may be forcibly disconnected from a distributed system if it is unresponsive for a period of time, or if a network partition separates one or more members into a group that is too small to act as the distributed system. When this property is set to "false" (the default), the member automatically shuts down and then restarts into a "reconnecting" state, where it periodically attempts to rejoin the distributed system. If it succeeds in reconnecting, the member rebuilds its view of the distributed system from existing members, and it receives a new distributed system ID. See also <a href="#jdbc_connection_attributes__section_D22407F736AE42AD85303DA732BED25D" class="xref noPageCitation">max-num-reconnect-tries</a>, <a href="#jdbc_connection_attributes__section_7E1E441D83344259A3916399663E30C8" class="xref noPageCitation">max-wait-time-reconnect</a>, and <a href="../../developers_guide/topics/server-side/fabricservice-reconnect.md#concept_22EE6DDE677F4E8CAF5786E17B4183A9" class="xref" title="A RowStore member may be forcibly disconnected from a distributed system if it is unresponsive for a period of time, or if a network partition separates one or more members into a group that is too small to act as the distributed system. If you start RowStore using the FabricService interface, you can use callback methods to perform actions during the reconnect process, or to cancel the reconnect process if necessary.">Handling Forced Disconnection</a>.

## Default Value

false

## Property Type

connection (boot)

## Prefix

gemfire.
