# shutdown

## Description

Stops SnappyData in the current process. A successful shutdown always results in a SQLException indicating that StopsSnappyData has shut down and that there is no longer a connection active.

`shutdown=true` overrides all other attributes that might be specified in the JDBC connection.

## Default Value

false

## Property Type

connection (boot)

## Prefix

n/a
