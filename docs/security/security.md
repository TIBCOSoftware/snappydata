# Managing Security

In the current release, TIBCO ComputeDB only supports LDAP authentication which allows users to authenticate against an existing LDAP directory service in your organization. LDAP (lightweight directory access protocol) provides an open directory access protocol that runs over TCP/IP. </br>This feature provides a secure way for users to use their existing login credentials (usernames and passwords) to access the cluster and data.

TIBCO ComputeDB uses mutual authentication between the TIBCO ComputeDB locator and subsequent TIBCO ComputeDB members that boot and join the distributed system. 

!!! Note
	
	* Currently, only LDAP based authentication and authorization is supported

	* The user launching the cluster becomes the admin user of the cluster.

	* All members of the cluster (leads, locators, and servers) must be started by the same user

<!--	* The TIBCO ComputeDB cluster and the Spark cluster (smart connector mode) must be secure-->
Refer to [User Names for Authentication, Authorization, and Membership](user_names_for_authentication_authorization_and_membership.md#user-names) for more information on how user names are treated by each system.

* [Launching the Cluster in Secure Mode](launching_the_cluster_in_secure_mode.md)

* [Specifying Encrypted Passwords in Conf Files or in Client Connections](specify_encrypt_passwords_conf_client.md)

* [Authentication - Connecting to a Secure Cluster](authentication_connecting_to_a_secure_cluster.md)
 
* [Authorization](authorization.md)

* [Implementing Row Level Security](row_level_security.md)

* [Configuring Network Encryption and Authentication using SSL](configuring_network_encryption_and_authentication_using_ssl.md)

* [Securing TIBCO ComputeDB Monitoring Console Connection](../configuring_cluster/securinguiconnection.md)

* [User Names for Authentication, Authorization, and Membership](user_names_for_authentication_authorization_and_membership.md)

