# gemfire.tx-disable-batching

## Description

Boolean that determines whether SnappyData detects conflicts in thin client transactions lazily or at operation time. With the default value of "false," SnappyData detects any conflicts in DML operations lazily. DML conflicts may be thrown by the system at some point later in the transaction (for example, even when executing queries or at commit time). Set this option to "true" on all data store members in your distributed system to immediately detect conflicts at operation time for thin clients. 

!!!Note 
	Enabling `gemfire.tx-disable-batching` can degrade performance significantly. Enable this option only after you have thoroughly tested the setting in your system, and you have determined that the performance tradeoff is necessary to provide immediate conflict detection with thin clients. </p>

## Default Value

false

## Property Type

system

## Prefix

n/a
