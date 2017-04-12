# mcast-flow-control

## Description


!!!Note 
	This setting controls only peer-to-peer communication. </p>

Tuning property for flow-of-control protocol for unicast and multicast no-ack UDP messaging. Compound property made up of three settings separated by commas: byteAllowance, rechargeThreshold, and rechargeBlockMs.

Valid values range from these minimums: 10000,0.1,500 to these maximums: no\_maximum ,0.5,60000.

## Default Value

1048576,0.25, 5000

## Property Type

connection (boot)

## Prefix

gemfire.
