# load-balance

## Description

Specifies whether load balancing is performed for the JDBC/ODBC client connection. With the default value ("true") clients are automatically connected to a less-loaded server if the locators are used for member discovery. 

!!! Note 
	- Load balancing is provided only for TIBCO ComputeDB distributed systems that use locators for member discovery.

	- When load balancing is enabled, clients may not be able to connect to a specific server even if they provide that server's unique port number for client connections. As a best practice, clients should always request connections using a locator address and port when load balancing is enabled.

With 1.1.0 release, the load-balance is set to false by default, when you connect to a server's hostname:port. And it is set to true, when you connect to a locator's hostname:port. The locator then redirects the client to a less-loaded server with which the client makes the connection. With 1.1.1 release, same behavior is implemented for ODBC driver as well. 

!!! Note
    	You must specify `load-balance=true` in ODBC properties, if the locator address and port is provided.

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
