# keepalive-count

## Description

Number of consecutive TCP keepalive probes that can be sent without receiving a response. After this count is reached, the client or server sending the probes declares that the recipient is offline. You can use this property with either a SnappyData client socket (to determine when a server is offline) or a SnappyData network server socket (to determine when clients go offline). 

!!!Note 
	Windows platforms do not support per-socket configuration for `keepalive-count`. As an alternative, you can configure a system-wide `keepalive-count` value in some versions of Windows. See [http://msdn.microsoft.com/en-us/library/windows/desktop/dd877220%28v=vs.85%29.aspx](http://msdn.microsoft.com/en-us/library/windows/desktop/dd877220%28v=vs.85%29.aspx). Windows Vista and later versions keep this value fixed at 10.

## Default Value

10

## Property Type

connection (boot)

## Prefix

snappydata.
