# CredentialInitializer

CredentialInitializer specifies the mechanism to obtain credentials for a peer.

CredentialInitializer is mandatory for peers when running in secure mode and custom implementation of UserAuthenticator is configured on the server or locator.

To configure a custom user authenticator on a server or locator, include the fully qualified class name of the implementation of UserAuthenticator in the `auth-provider` property.


