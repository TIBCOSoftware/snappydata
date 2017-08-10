# Managing Security

## Launching the Cluster in Secure Mode

LDAP (lightweight directory access protocol) provides an open directory access protocol that runs over TCP/IP. SnappyData can authenticate users against an existing LDAP directory service in your organisation.

An LDAP directory service can quickly authenticate using the user’s authentication credentials (name and password).

!!! Note:
	* Currently only LDAP based authentication and authorization is supported.

	* Ensure that user with administrative privileges launches the cluster. 

	* All members of the cluster (locators, servers and leads) must be started by the same user (with administrative priviledges)

## Connecting to a Secure Cluster