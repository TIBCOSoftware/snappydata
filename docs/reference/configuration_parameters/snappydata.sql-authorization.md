# snappydata.sql-authorization

## Description


!!!Warning
	This property is not supported in this release.

Enables or disables authorization in SnappyData. See <mark>[Configuring User Authorization]../../deploy_guide/Topics/security/cdevcsecure36595.md#cdevcsecure36595) </mark>. The default value of this property is "false." However, if you use the `-auth-provider` option to specify a client authentication mechanism when starting a SnappyData member, SQL authorization is enabled by default.

## Default Value

false

## Property Type

**system**

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
