# snappydata.client.single-hop-max-connections


## Description

If [snappydata.client.single-hop-max-connections](snappydata.client.single-hop-max-connections.md) is set to true, then this system property specifies the maximum number of single-hop client connections that a server can support. The default value of 5 creates enough connections for 5 single-hop clients to distribute queries to other SnappyData members in the cluster. Set this value to the maximum number of single-hop clients that you want to support at one time.

!!! Note 
	The actual number of connections that may be created for single-hop clients is determined by the number of members in the SnappyData distributed system. For example, if the SnappyData cluster has 10 members, then the default `snappydata.client.single-hop-max-connections` setting of 5 means that a maximum of 50 connections could be created for single-hop access (5 simultaneous single-hop clients connecting to all servers in the cluster). However, the actual connection resources are created only when necessary. 

<mark>See [Thin Client JDBC Driver Connections../../deploy_guide/Topics/thin-client-connecting.md#concept_234C554A61E94358B3D7A51A2B1A76F4).
</mark>
## Default Value

5

## Property Type

system

## Prefix

n/a
