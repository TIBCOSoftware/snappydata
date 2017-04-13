# snappydata.drda.securityMechanism

## Description

Restricts client connections based on the security mechanism. If set to a valid mechanism, then only those connections with the specified mechanism are allowed. If no value is set, then connections with any mechanism are allowed.

Possible values are:

-   USER\_ONLY\_SECURITY
-   CLEAR\_TEXT\_PASSWORD\_SECURITY
-   ENCRYPTED\_USER\_AND\_PASSWORD\_SECURITY
-   STRONG\_PASSWORD\_SUBSTITUTE\_SECURITY

## Default Value

None

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
