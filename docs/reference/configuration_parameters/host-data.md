# host-data

## Description

Specifies whether this SnappyData member will host table data.

Setting this value to `false` boots a SnappyData accessor member with the full SnappyData engine. The accessor (or peer client) participates in routing queries to other members, but it does not host replicated or partitioned data, and it does not persist the data dictionary.

Setting this value to `true` boots a SnappyData data store. Data stores host data, and they persist the data dictionary by default. All data stores in the same SnappyData cluster must use the same persistence settings (using the `persist-dd` boot property).

You should generally specify one or more server groups for the data store by including the `server-groups` attribute. By default, all new SnappyData data stores are added to the default server group.

## Default Value

true

## Property Type

connection (boot)

## Prefix

snappydata.
