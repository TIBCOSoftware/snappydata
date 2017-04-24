# keepalive-interval

## Description

TCP keepalive timeout interval in seconds. This is the time interval to wait between successive TCP keepalive probes if there is no response to the previous probe. You can use this property with either a SnappyData client socket (to determine when a server is offline) or a SnappyData network server socket (to determine when clients go offline). 

!!!Note 
	On Solaris platforms prior to r10, system-wide TCP keepalive settings must be changed to larger values (approximately 30 seconds) in order to detect server failures by clients and vice versa. See [http://docs.oracle.com/cd/E19082-01/819-2724/fsvdg/index.html](http://docs.oracle.com/cd/E19082-01/819-2724/fsvdg/index.html). This also applies to other non-Linux, non-Windows platforms. For example, see [http://www-01.ibm.com/support/docview.wss?uid=swg21231084](http://www-01.ibm.com/support/docview.wss?uid=swg21231084). </p>

## Default Value

1

## Property Type

connection (boot)

## Prefix

snappydata.
