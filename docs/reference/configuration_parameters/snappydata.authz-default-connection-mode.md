# snappydata.authz-default-connection-mode

## Description

If authorization is enabled, then this property sets the access mode for all users that are not listed in the `snappydata.authz-full-access-users` or `snappydata.authz-read-only-access-users` properties. Configure this property only if you do not intend to use the GRANT and REVOKE commands to manage privileges on SQL objects. The possible values are NOACCESS, READONLYACCESS, and FULLACCESS.

Keep in mind that using this property overrides any fine-grained privileges that are granted using the GRANT statement. For example, if you set this property to NOACCESS, then any user that is not listed under `snappydata.authz-full-access-users` or `snappydata.authz-read-only-access-users` has no access to SnappyData tables. You cannot use GRANT to give such a user additional privileges.

!!!Note:
	If you set the `snappydata.authz-default-connection-mode` property to *noAccess* or *readOnlyAccess*, you should allow at least one user read-write access. Otherwise, depending on the default connection authorization that you specify, your system may contain database objects that cannot be accessed or changed. You must specify that the user has access by specifying `snappydata.authz-full-access-users=username` on the command line when starting SnappyData; you cannot define the property in <span class="ph filepath">gemfirexd.properties</span>.

## Default Value

If you do not configure this property, then all users have full access (read/write privileges) unless the user is listed in `snappydata.authz-read-only-access-users` or have been given specific permissions with GRANT and REVOKE.

## Property Type

system

!!!Note 
	You must define this property as a Java system property (for example by using -J-D*property\_name*=*property\_value* with a `snappy-shell` utility, or by setting JAVA\_ARGS="-D*property\_name*=*property\_value*").</p>

## Prefix

n/a
