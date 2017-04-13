# snappydata.authz-read-only-access-users

## Description


!!!Warning
	This property is not supported in this release.

Defines the list of user names that have read-only access to SQL objects. This property is generally used when `snappydata.authz-default-connection-mode` is set to FULLACCESS. Configure this property only if you do not intend to use the GRANT and REVOKE commands to manage privileges on SQL objects. Any users that are listed in `snappydata.authz-read-only-access-users` have read-only access, regardless of whether any additional privileges were granted using the GRANT statement.

## Default Value

not set

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
