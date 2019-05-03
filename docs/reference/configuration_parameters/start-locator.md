# start-locator

## Description

If set, automatically starts a locator in the current process when the member connects to the distributed system and stops the locator when the member disconnects.

To use, specify the locator with an optional address or host specification and a required port number, in one of these formats:

```pre
start-locator=address[port1] 
```

```pre
start-locator=port1
```

* If you only specify the port, the address assigned to the member is used for the locator.

* If not already there, this locator is automatically added to the list of locators in this set of TIBCO ComputeDB properties.

<!--* If you set `start-locator`, do not also specify `mcast-port`. -->

## Default Value

not set

## Property Type

connection (boot)

## Prefix

gemfire.
