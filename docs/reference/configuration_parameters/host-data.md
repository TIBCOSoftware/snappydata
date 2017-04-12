# host-data

## Description

Specifies whether this RowStore member will host table data.

Setting this value to `false` boots a RowStore accessor member with the full RowStore engine. The accessor (or peer client) participates in routing queries to other members, but it does not host replicated or partitioned data, and it does not persist the data dictionary.

Setting this value to `true` boots a RowStore data store. Data stores host data, and they persist the data dictionary by default. All data stores in the same RowStore cluster must use the same persistence settings (using the `persist-dd` boot property).

You should generally specify one or more server groups for the data store by including the `server-groups` attribute. By default, all new RowStore data stores are added to the default server group.

## Default Value

true

## Property Type

connection (boot)

## Prefix

snappydata.
