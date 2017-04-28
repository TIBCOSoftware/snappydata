# snappydata.auth-ldap-search-base

## Description

!!!Warning
	This property is not supported in this release.

Use this property to limit the search space used when SnappyData verifies a user login ID. Specify the name of the context or object to search, that is a parameter to javax.naming.directory.DirContext.search(). For example:

``` pre
ou=ldapTesting,dc=pivotal,dc=com
```

By default, SnappyData tries to bind the anonymous user for searching when you configure snappydata.auth-ldap-search-base. If your LDAP server does not support anonymous binding, also configure [snappydata.auth-ldap-search-dn](snappydata.auth-ldap-search-dn.md) and [snappydata.auth-ldap-search-pw](snappydata.auth-ldap-search-pw.md).

## Default Value

not set

## Property Type

**system**

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
