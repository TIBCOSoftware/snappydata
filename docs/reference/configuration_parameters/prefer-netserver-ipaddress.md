# prefer-netserver-ipaddress

## Description

Determines whether locators (or intermediate servers, if locators are not used for discovery) provide server hostnames or server IP addresses to clients who are connecting to the distributed system. With the default setting (false), a locator provides hostnames to a client. Hostnames are generally preferable to use because it is more likely that an IP address may not be valid outside of a firewall, where clients typically reside. However, if you configure a system using only IP addresses, or there is a likelihood of a mismatch between hostnames and IP addresses (either due to misconfigured `/etc/hosts` files or DHCP hostnames are overwritten), then set this property to true to provide IP addresses for client connections.

!!! Note 
	This is a server-side boot property. You cannot use this property in a thin client connection string.</p>

## Default Value

false

## Property Type

connection (boot)

## Prefix

snappydata.
