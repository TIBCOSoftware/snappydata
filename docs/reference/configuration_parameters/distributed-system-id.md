# distributed-system-id

## Description

Identifier used to distinguish messages from different distributed systems.

When starting a locator, set this property to a unique value for the cluster in a multi-site (WAN) configuration. Valid values are integers in the range -1...255. All locators for a single cluster must use the same value.

-1 means no setting. Individual SnappyData members can use the setting of -1 and connect to one or more locators that specify a unique distribute-system-id.

## Default Value

-1

## Property Type

connection (boot)

## Prefix

gemfire.
