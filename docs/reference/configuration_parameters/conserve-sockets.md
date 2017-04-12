# conserve-sockets

## Description

Specifies whether sockets are shared by the system memberâs threads. If true, threads share, and a minimum number of sockets are used to connect to the distributed system. If false, every application thread has its own sockets for distribution purposes. You can override this setting for individual threads inside your application. Where possible, it is better to set conserve-sockets to true and enable the use of specific extra sockets in the application code if needed. The length of time a thread can have exclusive access to a socket can be configured with `socket-lease-time`. 

!!!Note 
	WAN deployments increase the messaging demands on a RowStore system. To avoid hangs related to WAN messaging, always set conserve-sockets=false for RowStore members that participate in a WAN deployment. See <a href="../../config_guide/topics/gateway-hubs/wan-prerequisites.md#concept_6459DD0925D444329DCD234F48C36CE8" class="xref" title="Note these prerequisites for configuring and using replication between two RowStore clusters.">Prerequisites for WAN Replication</a>. </p>

## Default Value

true

## Property Type

connection (boot)

## Prefix

gemfire.
