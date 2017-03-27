# UserAuthenticator

The UserAuthenticator interface provides operations to authenticate a user's credentials for connection to a database.

User authentication schemes can be implemented with this interface and registered at start-up time.

If an application requires its own authentication scheme, it can implement this interface and register as the authentication scheme that SnappyData should call upon connection requests to the system.

A typical example is the implementation of user authentication with LDAP, Sun NIS+, or even Windows User Domain, using this interface.

!!!Note:
	You can specify additional connection attributes can be specified on the database connection URL and/or Properties object on jdbc connection. Values for these attributes can be retrieved at runtime by the (specialized) authentication scheme to further help user authentication, if one needs information in addition to user, password, and database name.
