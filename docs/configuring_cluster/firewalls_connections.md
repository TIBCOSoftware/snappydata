# Firewalls and Connections

You may face possible connection problems that can result from running a firewall on your machine.

SnappyData is a network-centric distributed system, so if you have a firewall running on your machine it could cause connection problems. For example, your connections may fail if your firewall places restrictions on inbound or outbound permissions for Java-based sockets. You may need to modify your firewall configuration to permit traffic to Java applications running on your machine. The specific configuration depends on the firewall you are using.

As one example, firewalls may close connections to SnappyData due to timeout settings. If a firewall senses no activity in a certain time period, it may close a connection and open a new connection when activity resumes, which can cause some confusion about which connections you have.


**Firewall Considerations**

You can configure and limit port usage for situations that involve firewalls, for example, between client-server or server-server connections.

-   **[Firewalls and Ports](firewalls_ports.md)**
    Make sure your port settings are configured correctly for firewalls.
-   **[Locators, Gateway Receivers, and Ports](locators_gateways.md)**
    The ephemeral port range and TCP port range for locators must be accessible to members through the firewall. The full range of port values for gateway receivers also must be accessible from across the WAN.


