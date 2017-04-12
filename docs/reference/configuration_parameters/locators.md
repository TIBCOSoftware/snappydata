# locators


## Description

List of locators used by system members. The list must be configured consistently for every member of the distributed system. If the list is empty, locators are not used.

For each locator, provide a host name and/or address (separated by '@', if you use both), followed by a port number in brackets. Examples:

``` pre
locators=address1[port1],address2[port2]
```

``` pre
locators=hostName1@address1[port1],name2@address2[port2]
```

``` pre
locators=hostName1[port1],name2[port2]
```

!!! Note
	On multi-homed hosts, this last notation uses the default address. If you use bind addresses for your locators, explicitly specify the addresses in the locators list - do not use just the hostname. 

## Default Value

not set

## Property Type

connection (boot)

## Prefix

gemfire.
