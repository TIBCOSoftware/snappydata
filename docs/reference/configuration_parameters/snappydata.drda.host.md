# snappydata.drda.host

## Description

Network interface on which the network server listens.

This property allows multiple members of a Network Server to run on a single machine, each using its own unique host:port combination.

If the property is set to 0.0.0.0, the Network Server listens on all interfaces. Ensure that you are running under the security manager and that user authorization is enabled before you enable remote connections with this property

## Default Value

The Network Server listens only on the loopback address (localhost).

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
