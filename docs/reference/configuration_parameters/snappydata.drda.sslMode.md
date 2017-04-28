# snappydata.drda.sslMode

## Description

Determines whether client connections to the member are encrypted or not, and whether certificate-based peer authentication is enabled. Possible values are Off, Basic, and peerAuthentication.

Thin clients connecting to a server that implements SSL should specify the connection property [ssl](ssl.md).

## Default Value

Off

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
