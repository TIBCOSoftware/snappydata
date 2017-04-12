# snappydata.drda.maxThreads

## Description

Maximum number of connection threads that the Network Server allocates. If the maximum number of threads are allocated, then connections are shared using the `snappydata.drda.timeSlice` property to switch between connections. This setting can be enabled using NetworkInterface as well, but the Network Server must be restarted for the change to take effect.

## Default Value

0 (unlimited)

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
