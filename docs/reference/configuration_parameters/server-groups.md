# server-groups

## Description

One or more server groups in which the SnappyData member participates. Use a comma-separated list of server group names to supply multiple values. 

!!!Note 
	SnappyData converts server group names to all-uppercase letters before storing the values in the SYS.MEMBERS table. DDL statements and procedures automatically convert any supplied server group values to all-uppercase letters. However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.</p>

Server groups are used with SnappyData DDL statements, and also for executing data-aware procedures. If you do not use the `server-groups` attribute, or if you do not specify a value for the attribute, then the SnappyData member is added only to the default server group. The default server group includes all of the SnappyData members that participate in the distributed system.

SnappyData members that do not host data (`host-data=false`) can still participate in server groups in order to provide routing information for data-aware procedures that target specific server groups.

## Default Value

not set

## Property Type

connection (boot)

## Prefix

snappydata.
