<a id="howto-external-client"></a>
# How to Connect to the Cluster from External Network

You can also connect to the SnappyData cluster from a different network as a client (DbVisualizer, SQuirreL SQL etc.). </br>For example, to connect to a cluster on AWS from your local machine set the following properties in the *conf/locators* and *conf/servers* files:

* `client-bind-address`: Set the hostname or IP address to which the locator or server listens on, for JDBC/ODBC/thrift client connections.

* `hostname-for-clients`: Set the IP address or host name that this server/locator sends to the JDBC/ODBC/thrift clients to use for the connection. The default value causes the `client-bind-address` to be given to clients. </br> This value can be different from `client-bind-address` for cases where the servers/locators are behind a NAT firewall (AWS for example) where `client-bind-address` needs to be a private one that gets exposed to clients outside the firewall as a different public address specified by this property. In many cases, this is handled by the hostname translation itself, that is, the hostname used in `client-bind-address` resolves to internal IP address from inside and to the public IP address from outside, but for other cases, this property is required.

	!!! Note
    	By default, the locator or server binds to localhost. You may need to set either or both these properties to enable connection from external clients. If not set, external client connections may fail.

* Port Settings: Locator or server listens on the default port 1527 for client connections. Ensure that this port is open in your firewall settings. <br> You can also change the default port by setting the `client-port` property in the *conf/locators* and *conf/servers*.

!!! Note
	For ODBC clients, you must use the host and port details of the server and not the locator.
