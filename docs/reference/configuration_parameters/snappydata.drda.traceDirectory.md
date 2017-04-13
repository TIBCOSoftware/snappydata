# snappydata.drda.traceDirectory

## Description

Location of the tracing directory on the server. This setting can be enabled using NetworkInterface as well, but the Network Server must be restarted for the change to take effect.

## Default Value

Uses the snappydata.system.home property (if set). Otherwise, uses the current directory.

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
