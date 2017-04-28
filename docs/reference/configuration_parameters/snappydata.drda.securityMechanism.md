# snappydata.drda.securityMechanism

## Description

Restricts client connections based on the security mechanism. If set to a valid mechanism, then only those connections with the specified mechanism are allowed. If no value is set, then connections with any mechanism are allowed.

Possible values are:

-   USER_ONLY_SECURITY
-   CLEAR_TEXT_PASSWORD_SECURITY
-   ENCRYPTED_USER_AND_PASSWORD_SECURITY
-   STRONG_PASSWORD_SUBSTITUTE_SECURITY

## Default Value

None

## Property Type

system 

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property_name*=*property_value* with a `snappy` utility, or by setting JAVA_ARGS="-D*property_name*=*property_value*").</p>

## Prefix

n/a
