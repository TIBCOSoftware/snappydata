# snappydata.datadictionary.allow-startup-errors

## Description

Enables a SnappyData member to start up, ignoring DDL statements that fail during member initialization. This property enables you to resolve startup problems manually, after forcing the member to start. Typical DDL initialization problems occur when a required disk store file is unavailable, or when SnappyData cannot initialize a DBSynchronizer configuration due to the external RDBMS being unavailable. Use `snappydata.datadictionary.allow-startup-errors` to drop and recreate the disk store or DBSynchronizer configuration after startup. See also [Member Startup Problems](../../troubleshooting.md).

## Default Value

false

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
