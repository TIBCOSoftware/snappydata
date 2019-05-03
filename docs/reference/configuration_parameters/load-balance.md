# load-balance

## Description

Specifies whether load balancing is performed for the JDBC client connection. 

With the default value ("true") clients are automatically connected to a less-loaded server if locators are used for member discovery. 

!!! Note 
	- Load balancing is provided only for TIBCO ComputeDB distributed systems that use locators for member discovery.

	- When load balancing is enabled, clients may not be able to connect to a specific server even if they provide that server's unique port number for client connections. As a best practice, clients should always request connections using a locator address and port when load balancing is enabled.

If a JDBC client needs to connect to a specific member, set `load-balance` to "false" in the connection string and specify the connection details for a specific TIBCO ComputeDB member, rather than a locator. 

For example:

```pre
snappy> connect client 'server_hostname:server_port/;load-balance=false'
```

## Default Value

true

## Property Type

connection

## Prefix

n/a
