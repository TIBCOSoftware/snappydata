# disable-streaming

## Description

Disables results streaming on the query node for this connection. Disabling streaming degrades performance and requires more memory for large query results, but provides more predictable results if a SnappyData member happens to go down while iterating over a ResultSet.

When this property is not enabled, a SQLException with state <span class="keyword apiname">GFXD_NODE_SHUTDOWN</span> is thrown if a member goes offline in the middle of ResultSet iteration; in this case, the application has to retry the query. When this property is enabled, SnappyData waits for at least one result from each member and does not group results from members. This provides transparent failover if a member goes offline.

## Default Value

false

## Property Type

connection

## Prefix

snappydata.
