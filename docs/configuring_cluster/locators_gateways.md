# Locators and Ports

The ephemeral port range and TCP port range for locators must be accessible to members through the firewall.

Locators are used in the peer-to-peer cache to discover other processes. They can be used by clients to locate servers as an alternative to configuring clients with a collection of server addresses and ports.

Locators have a TCP/IP port that all members must be able to connect to. They also start a distributed system and so need to have their ephemeral port range and TCP port accessible to other members through the firewall.

Clients need only be able to connect to the locator's locator port. They don't interact with the locator's distributed system; clients get server names and ports from the locator and use these to connect to the servers. For more information, see Using Locators.