# snappydata.drda.timeSlice

## Description

Number of milliseconds that each connection uses before yielding to another connection. This property is effective only if `snappydata.drda.maxThreads` is set greater than zero.

The server must be restarted for changes to take effect.

## Default Value

0

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
