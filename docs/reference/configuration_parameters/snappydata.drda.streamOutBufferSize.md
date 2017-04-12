# snappydata.drda.streamOutBufferSize

## Description

Configures the size of the buffer used for streaming BLOB/CLOB data from the server to a client. If the configured size is 0 or less, then the buffer is not created.

## Default Value

0

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
