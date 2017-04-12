# mcast-recv-buffer-size

## Description

!!!Note
	This setting controls only peer-to-peer communication and does not apply to client/server or multi-site communication. </br> Size of the socket buffer used for incoming multicast transmissions. You should set this high if there will be high volumes of messages.

The default setting is higher than the default OS maximum buffer size on Unix, which should be increased to at least 1 megabyte to provide high-volume messaging on Unix systems.

Valid values are in the range 2048.. OS\_maximum.

## Default Value

1048576

## Property Type

connection (boot)

## Prefix

gemfire.
