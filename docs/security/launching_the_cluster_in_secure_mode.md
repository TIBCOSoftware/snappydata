# Launching the Cluster in Secure Mode

TIBCO ComputeDB uses mutual authentication between the TIBCO ComputeDB locator and subsequent TIBCO ComputeDB members that boot and join the distributed system.

<a id= authproperties> </a>
## Authentication Properties

To enable LDAP authentication, set the following authentication properties in the [configuration files](../configuring_cluster/configuring_cluster.md) **conf/locators**, **conf/servers**, and **conf/leads** files.

* `auth-provider`: The authentication provider. Set the `auth-provider` property to `LDAP`, to enable LDAP for authenticating all distributed system members as well as clients to the distributed system.

*	`server-auth-provider`: Peer-to-peer authentication of cluster members is configured in the TIBCO ComputeDB cluster. You can set `server-auth-provider` property to **NONE** if you want to disable the peer-to-peer authentication.

* `user`: The user name of the administrator starting the cluster

* `password`: The password of the administrator starting the cluster

* `J-Dgemfirexd.auth-ldap-server`: Set this property to the URL to the LDAP server.

* `J-Dgemfirexd.auth-ldap-search-base`: Use this property to limit the search space used when TIBCO ComputeDB verifies a user login ID. Specify the name of the context or object to search, that is a parameter to `javax.naming.directory.DirContext.search()`. 

* `J-Dgemfirexd.auth-ldap-search-dn`: If the LDAP server does not allow anonymous binding (or if this functionality is disabled), specify the user distinguished name (DN) to use for binding to the LDAP server for searching.

* `J-Dgemfirexd.auth-ldap-search-pw`: The password for the LDAP search user which is used for looking up the DN indicated by configuration parameter `Dgemfirexd.auth-ldap-search-dn`. 

## Example - Launching Locator in Secure Mode

In the below example, we are launching the locator in secure mode, which communicates with the LDAP server at localhost listening on port 389.
```pre
localhost -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
          -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          -J-Dgemfirexd.auth-ldap-search-pw=user123
```

!!! Note
	You must specify `gemfirexd.auth-ldap-*` properties as Java system properties by prefixing '-J-D'.
    
	If you use SSL-encrypted LDAP and your LDAP server certificate is not recognized by a valid Certificate Authority (CA), you must create a local trust store for each TIBCO ComputeDB member and import the LDAP server certificate to the trust store. See the document on [Creating a Keystore]( http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore) for more information.

	Specify the javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword system properties when you start individual TIBCO ComputeDB members. For example:

		localhost -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/  \
			-J-Dgemfirexd.auth-ldap-server=ldaps://ldapserver:636/ -user=user_name -password=user_pwd \
     			-J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com \
          		-J-Dgemfirexd.auth-ldap-search-pw=user123
          		-J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com \
			-J-Djavax.net.ssl.trustStore=/Users/user1/snappydata/keystore_name \
			-J-Djavax.net.ssl.trustStorePassword=keystore_password

	javax.net.ssl.trustStore and javax.net.ssl.trustStorePassword must be specified as Java system properties (using the -J option on the Snappy Shell).

