# enforce-unique-host

## Description

Determines whether SnappyData puts redundant copies of the same data in different members running on the same physical machine. By default, SnappyData tries to put redundant copies on different machines, but it puts them on the same machine if no other machines are available. </br>
Setting this property to **true** prevents this and requires different machines for redundant copies.

## Usage

In the **conf/servers** file you can set this as:

```pre
localhost -locators=localhost:3241,localhost:3242 -gemfire.enforce-unique-host=true
```

## Default Value

false

## Property Type

connection (boot)

## Prefix

gemfire.

