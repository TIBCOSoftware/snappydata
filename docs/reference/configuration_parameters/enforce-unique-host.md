# enforce-unique-host

## Description

Boolean that determines whether SnappyData puts redundant copies of the same data in different members running on the same physical machine. By default, SnappyData tries to put redundant copies on different machines, but it puts them on the same machine if no other machines are available. Setting this property to true prevents this and requires different machines for redundant copies.

## Default Value

false

## Property Type

connection (boot)

## Prefix

gemfire.
