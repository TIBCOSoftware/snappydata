# gemfirexd.query-cancellation-interval

## Description

After used memory used passes a critical limit, TIBCO ComputeDB begins cancelling queries to free memory. This attribute specifies the period in milliseconds after which TIBCO ComputeDB cancels a query during periods of critical memory usage. With the default value, TIBCO ComputeDB cancels a query every 100 milliseconds when necessary to free memory.

## Default Value

100

## Property Type

connection (boot)

## Prefix

n/a
