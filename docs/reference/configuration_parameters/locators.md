# locators


## Description

List of locators used by system members. The list must be configured consistently for every member of the distributed system. If the list is empty, locators are not used.

For each locator, provide a host name and/or address (separated by '@', if you use both), followed by a port number in brackets. Examples:

```pre
locators=address1[port1],address2[port2]
```

```pre
locators=hostName1@address1[port1],name2@address2[port2]
```

```pre
locators=hostName1[port1],name2[port2]
```

!!! Note
	On multi-homed hosts, this last notation uses the default address. If you use bind addresses for your locators, explicitly specify the addresses in the locator's list - do not use just the hostname. 

## Usage
To start multiple locators in a cluster modify the following files: 

- ***conf/locators***

        localhost -peer-discovery-address=localhost -peer-discovery-port=3241 -locators=localhost:3242
        localhost -peer-discovery-address=localhost -peer-discovery-port=3242 -locators=localhost:3241

- ***conf/servers***

	    localhost -locators=localhost:3241,localhost:3242


- ***conf/leads***

	    localhost -locators=localhost:3241,localhost:3242


## Default Value

not set

## Property Type

connection (boot)

## Prefix

gemfire.
