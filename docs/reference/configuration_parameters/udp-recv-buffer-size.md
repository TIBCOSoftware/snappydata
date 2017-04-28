# udp-recv-buffer-size

## Description

Size of the socket buffer used for incoming UDP point-to-point transmissions. If disable-tcp is false, a reduced buffer size of 65535 is used by default.

The default setting of 1048576 is higher than the default OS maximum buffer size on Unix, which should be increased to at least 1MB to provide high-volume messaging on Unix systems.

Valid values are in the range 2048.. OS_maximum.

## Default Value

1048576

## Property Type

connection (boot)

## Prefix

gemfire.
