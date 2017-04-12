# snappydata.auth-ldap-search-filter


## Description

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
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
