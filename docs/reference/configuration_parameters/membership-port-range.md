# membership-port-range

## Description

The range of ports available for unicast UDP messaging and for TCP failure detection. Specify this value as two integers separated by a minus sign. Different members can use different ranges.

TIBCO ComputeDB randomly chooses two unique integers from this range for the member, one for UDP unicast messaging and the other for TCP failure detection messaging. Additionally, the system uniquely identifies the member using the combined host IP address and UDP port number.

You may want to restrict the range of ports that TIBCO ComputeDB uses so the product can run in an environment where routers only allow traffic on certain ports.

## Default Value

1024-65535

## Property Type

connection (boot)

## Prefix

gemfire.
