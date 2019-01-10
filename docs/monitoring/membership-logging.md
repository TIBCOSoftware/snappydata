# Product Usage Logging


Each SnappyData locator creates a log file that record the different membership views of the SnappyData distributed system. You can use the contents of these log files to verify that your product usage is compliance with your SnappyData license.

The locator membership view log file is stored in the locator member directory using the file format <span class="ph filepath">locator*XXXX*views.log</span>, where *XXXX* is the port number that the locator uses for peer connections. A locator records a membership view log entry when the it starts, and adds a new entry each time a member joins the system. Each time a new data store joins the system, a log entry records the total number of servers and clients in the system at that time. For example:

```pre
[info 2014/01/29 16:43:34.493 PST <CacheServerLauncher#serverConnector> tid=0xe] Log opened with new distributed system connection.  View(creator=ward(14487)<v0>:4720, viewId=0, [ward(14487)<v0>:4720])

[info 2014/01/29 16:44:35.421 PST <DM-MemberEventInvoker> tid=0x1c] A new member joined: ward(14501)<v1>:21541.  View(creator=ward(14487)<v0>:4720, viewId=1, [ward(14487)<v0>:4720, ward(14501)<v1>:21541])

[info 2014/01/29 16:44:52.986 PST <DM-MemberEventInvoker> tid=0x1c] A new member joined: ward(14512)<v3>:52370.  View(creator=ward(14487)<v0>:4720, viewId=3, [ward(14487)<v0>:4720, ward(14512)<v3>:52370])

[info 2014/01/29 16:44:57.765 PST <Pooled High Priority Message Processor 2> tid=0x4c] server summary: 1 cache servers with 0 client connection load
  current cache servers : ward(14512)<v3>:52370 

[info 2014/01/29 17:06:40.752 PST <DM-MemberEventInvoker> tid=0x1c] A new member joined: ward(14578)<v4>:5088.  View(creator=ward(14487)<v0>:4720, viewId=4, [ward(14487)<v0>:4720, ward(14512)<v3>:52370, ward(14578)<v4>:5088])

[info 2014/01/29 17:06:45.549 PST <Pooled High Priority Message Processor 1> tid=0x47] server summary: 2 cache servers with 0 client connection load
  current cache servers : ward(14512)<v3>:52370 ward(14578)<v4>:5088 

[info 2014/01/29 17:09:38.378 PST <DM-MemberEventInvoker> tid=0x1c] A new member joined: ward(14604)<v6>:8818.  View(creator=ward(14487)<v0>:4720, viewId=6, [ward(14487)<v0>:4720, ward(14578)<v4>:5088, ward(14604)<v6>:8818])

[info 2014/01/29 17:09:43.367 PST <Pooled High Priority Message Processor 2> tid=0x4c] server summary: 2 cache servers with 0 client connection load
  current cache servers : ward(14578)<v4>:5088 ward(14604)<v6>:8818 
```

The above output shows that some data stores left the distributed system and others joined the system, because the last membership detail messages both show 2 servers in the system. Note that no log entries are created when a member leaves the system.

By default, the size of the membership view log file is limited to 5MB. You can change the maximum file size using the <a href="../../reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes__sec_maxviewlogsize" class="xref noPageCitation">max\_view\_log\_size</a> property. After the maximum file size is reached, the locator deletes the log file and begins recording messages in a new file.


