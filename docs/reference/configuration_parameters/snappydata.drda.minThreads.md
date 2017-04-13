# snappydata.drda.minThreads

## Description

Minimum number of connection threads that the Network Server allocates. This setting can be enabled using NetworkInterface as well, but the Network Server must be restarted for the change to take effect.

## Default Value

On-demand allocation.

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
