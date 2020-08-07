# gemfirexd.datadictionary.allow-startup-errors

## Description

Enables a TIBCO ComputeDB member to start up, ignoring DDL statements that fail during member initialization. This property enables you to resolve startup problems manually, after forcing the member to start. Typical DDL initialization problems occur when a required disk store file is unavailable, or when TIBCO ComputeDB cannot initialize a DBSynchronizer configuration due to the external RDBMS being unavailable. Use `gemfirexd.datadictionary.allow-startup-errors` to drop and recreate the disk store or DBSynchronizer configuration after startup. <!-- See also [Member Startup Problems](../../troubleshooting.md).-->

## Default Value

false

## Property Type

system

!!! Note
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
