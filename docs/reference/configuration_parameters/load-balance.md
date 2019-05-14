# load-balance

## Description

Specifies whether load balancing is performed for the JDBC client connection. 

With the default value ("true") clients are automatically connected to a less-loaded server if locators are used for member discovery. 

!!! Note 
	- Load balancing is provided only for TIBCO ComputeDB distributed systems that use locators for member discovery.

	- When load balancing is enabled, clients may not be able to connect to a specific server even if they provide that server's unique port number for client connections. As a best practice, clients should always request connections using a locator address and port when load balancing is enabled.

With 1.1.0 release, the `load-balance` is set to **false**, by default, in the connection string. So now you must specify the connection details for a specific SnappyData member, other than a locator.
If you want to connect the JDBC client to a locator, then set this property to true. The locator then redirects the client to a less-loaded server with which the client makes the connection.

For example:

```pre
snappy> connect client 'server_hostname:1527;load-balance=false'
```

## Default Value

false

## Property Type

connection

## Prefix

n/a
