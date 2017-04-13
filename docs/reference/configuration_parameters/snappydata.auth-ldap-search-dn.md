# snappydata.auth-ldap-search-dn

## Description

!!!Warning
	This property is not supported in this release.

If the LDAP server does not allow anonymous binding (or if this functionality is disabled), specify the user distinguished name (DN) to use for binding to the LDAP server for searching. For example:

``` pre
uid=guest,o=pivotal.com
```

## Default Value

Null

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
