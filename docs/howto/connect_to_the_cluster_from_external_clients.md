<a id="howto-external-client"></a>
## How to Connect to the Cluster from an External Network

You can also connect to the SnappyData cluster from a different network as a client (DbVisualizer, SQuirreL SQL etc.). </br>For example, to connect to a cluster on AWS from your local machine set the following properties in the *conf/locators* and *conf/servers* files:

* `client-bind-address`: Set the hostname or IP address to which the locator or server binds. 

* `hostname-for-clients`: Set the IP address or host name that this server/locator listens on, for client connections. Setting a specific `hostname-for-clients` will cause locators to use this value when telling clients how to connect to this server. The default value causes the `bind-address` to be given to clients.

	!!! Note: 
    	By default, the locator or server binds to localhost. You may need to set either or both these properties to enable connection from external clients. If not set, external client connections may fail.

* Port Settings: Locator or server listens on the default port 1527 for client connections. Ensure that this port is open in your firewall settings. <br> You can also change the default port by setting the `client-port` property in the *conf/locators* and *conf/servers*.

!!! Note: 
	For ODBC clients, you must use the host and port details of the server and not the locator.
