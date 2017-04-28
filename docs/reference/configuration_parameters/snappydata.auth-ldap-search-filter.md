# snappydata.auth-ldap-search-filter


## Description

!!!Warning
	This property is not supported in this release.

Specifies a user's objectClass, used to narrow the LDAP search. For example:

``` pre
objectClass=person
```

<mark>See [Configuring SnappyData to Search for DNs ../../deploy_guide/Topics/security/ldap-guest-access.md#cdevcsecure876908). </mark>

## Default Value

(&(objectClass=inetOrgPerson)(uid=%USERNAME%))

## Property Type

**system **

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
