# snappydata.authz-full-access-users

## Description

Defines the list of user names that have full access to SQL objects. Users that are listed in `snappydata.authz-full-access-users` have full access regardless of whether specific privileges were taken away using the REVOKE statement.

## Default Value

not set

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
