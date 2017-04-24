# keepalive-idle

## Description

TCP keepalive idle timeout in seconds. This is the idle time after which a TCP keepalive probe is sent over the socket to determine if the other side of the socket is alive. You can use this property with either a SnappyData client socket (to determine when a server is offline) or a SnappyData network server socket (to determine when clients go offline). 

!!!Note 
	On Solaris platforms prior to r10, system-wide TCP keepalive settings must be changed to larger values (approximately 30 seconds) in order to detect server failures by clients and vice versa. See [http://docs.oracle.com/cd/E19082-01/819-2724/fsvdg/index.html](http://docs.oracle.com/cd/E19082-01/819-2724/fsvdg/index.html). This also applies to other non-Linux, non-Windows platforms. For example, see [http://www-01.ibm.com/support/docview.wss?uid=swg21231084](http://www-01.ibm.com/support/docview.wss?uid=swg21231084). </p>

## Default Value

20

## Property Type

connection (boot)

## Prefix

snappydata.
