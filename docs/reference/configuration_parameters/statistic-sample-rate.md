# statistic-sample-rate

## Description

Boot property that specifies how often to sample statistics, in milliseconds.</br>
Valid values are in the range 1000..60000.

!!! Note
	If the value is set to less than 1000, the rate will be set to 1000 because the VSD tool does not support sub-second sampling.

## Default Value

1000

## Property Type

connection (boot)

## Prefix

gemfire.
