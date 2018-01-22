# Firewalls and Ports

<a id="port-setting"></a>
Make sure your port settings are configured correctly for firewalls. For each SnappyData member, there are two different port settings you may need to be concerned with regarding firewalls:

-   The port that the server or locator listens on for client connections. This is configurable using the `-client-port` option to the snappy server or snappy locator command.

-   The peer discovery port. SnappyData members connect to the locator for peer-to-peer messaging. The locator port is configurable using the `-peer-discovery-port` option to the snappy server or snappy locator command.

    By default, SnappyData servers and locators discover each other on a pre-defined port (10334) on the localhost.

**Limiting Ephemeral Ports for Peer-to-Peer Membership**

By default, SnappyData utilizes *ephemeral* ports for UDP messaging and TCP failure detection. Ephemeral ports are temporary ports assigned from a designated range, which can encompass a large number of possible ports. When a firewall is present, the ephemeral port range usually must be limited to a much smaller number, for example six. If you are configuring P2P communications through a firewall, you must also set each the tcp port for each process and ensure that UDP traffic is allowed through the firewall.

**Properties for Firewall and Port Configuration**

This following tables contain properties potentially involved in firewall behavior, with a brief description of each property. The [Configuration Properties](../reference/configuration_parameters/config_parameters.md) section contains detailed information for each property.

| Configuration Area | Property or Setting | Definition |
|--------|--------|--------|
|peer-to-peer config|[locators](../reference/configuration_parameters/locators.md)|The list of locators used by system members. The list must be configured consistently for every member of the distributed system.|
|peer-to-peer config|[membership-port-range](../reference/configuration_parameters/membership-port-range.md)|The range of ephemeral ports available for unicast UDP messaging and for TCP failure detection in the peer-to-peer distributed system.|
|member config|[-J-Dgemfirexd.hostname-for-clients](../configuring_cluster/property_description.md#host-name)|Hostname or IP address to pass to the client as the location where the server is listening.|
|member config|[client-port](../reference/command_line_utilities/store-run/) option to the [snappy server](../configuring_cluster/configuring_cluster.md#configuring-data-servers) and [snappy locator](../configuring_cluster/configuring_cluster.md#configuring-locators) commands|Port that the member listens on for client communication.|

| Port Name | Related Configuration Setting |Default Port |
|--------|--------|--------|
|Locator|[locator command](../configuring_cluster/configuring_cluster.md#configuring-locators)|10334|
|Membership Port Range|[membership-port-range](../reference/configuration_parameters/membership-port-range.md)|1024 to 65535|