# MEMBERS

A SnappyData virtual table that contains information about each distributed system member.

!!! Note
	SnappyData converts server group names to all uppercase letters before storing the values in the SYS.MEMBERS table. DDL statements and procedures automatically convert any supplied server group values to all uppercase letters. However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.


|Column Name|Type |Length |Nullable|Contents|
| ------------ | ------------- | ------------ | ------------ | ------------- |
|ID|VARCHAR|128|No|The unique ID of the member. This ID has the format: <br>`hostname(process_id);member_number;:udp_port/tcp_port`<br>For example:<br>10.0.1.31(66878);v0;:41715/63386|
|KIND  |VARCHAR   | 24 |No   | Specifies the type of SnappyData member process: <br> * datastore—A member that hosts data.<br> * peer—A member that does not host data.<br> * locator—Provides discovery services for a cluster.<br> Member types can also be qualified with additional keywords <br>  * normal—The member can communicate with other members in a cluster. <br> * loner—The member is standalone and cannot communicate with other members. Loners use no locators for discovery.<br> * admin—The member also acts as a JMX manager node. |
|HOSTDATA  | BOOLEAN  | | Yes  |A value of ‘1’ indicates that this member is a data store and can host data. Otherwise, the member is a peer client with no hosted data. |	
|ISELDER  | BOOLEAN  |  |No |	Is this the eldest member of the distributed system. Typically, this is the member who first joins the cluster.|
|IPADDRESS  |  VARCHAR |64  |   Yes| The fully-qualified hostname/IP address of the member.|
|HOST   |VARCHAR  | 128  | Yes | The fully-qualified hostname of the member.|
| PID  |INTEGER  |10   | No |The member process ID. |
|PORT   |INTEGER  | 10  | No | The member UDP port.|
|ROLES   |VARCHAR  |128 | No |Not used. |
|NETSERVERS   |VARCHAR  |32672|No  |Host and port information for Network Servers that are running on SnappyData members. |
|LOCATOR   |VARCHAR  |32672 | No |Host and port information for locator members. |
|SERVERGROUPS   |VARCHAR  | 32672 |No |A comma-separated list of server groups of which this member is a part. <br> **Note**: SnappyData converts server group names to all uppercase letters before storing the values in the SYS.MEMBERS table. DDL statements and procedures automatically convert any supplied server group values to all uppercase letters. However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.|
|SYSTEMPROPS   |CLOB  | 2147483647  |No  | A list of all system properties used to start this member. This includes properties such as the classpath, JVM version, and so forth.|	
|GEMFIREPROPS   |CLOB  |2147483647   | No  |The names and values of GemFire core system properties that the member uses. |	
|BOOTPROPS   |CLOB  |2147483647   | No |All of the SnappyData boot properties names and values that a member uses.|	


**Example** </br>

```pre
snappy> select * from SYS.MEMBERS;
ID                           |KIND             |STATUS |HOSTDATA|ISELDER|IPADDRESS |HOST     |PID        |PORT       |ROLES|NETSERVERS               |THRIFTSERVERS            |LOCATOR         |SERVERGROUPS               |MANAGERINFO              |SYSTEMPROPS    |GEMFIREPROPS   |BOOTPROPS      
---------------------------------------------------------------------------------------------------------------- 
127.0.0.1(5687)<v1>:47719    |datastore(normal)|RUNNING|true    |false  |/127.0.0.1|localhost|5687       |47719      |     |localhost/127.0.0.1[1528]|localhost/127.0.0.1[1528]|NULL            |                           |Managed Node             |
--- System Pr&|
--- GemFire P&|
--- GemFireXD&
127.0.0.1(5877)<v2>:10769    |accessor(normal) |RUNNING|false   |false  |/127.0.0.1|localhost|5877       |10769      |     |                         |                         |NULL            |IMPLICIT_LEADER_SERVERGROUP|Managed Node             |
--- System Pr&|
--- GemFire P&|
--- GemFireXD&
127.0.0.1(5548)<ec><v0>:21415|locator(normal)  |RUNNING|false   |true   |/127.0.0.1|localhost|5548       |21415      |     |localhost/127.0.0.1[1527]|localhost/127.0.0.1[1527]|127.0.0.1[10334]|                           |Manager Node: Not Running|
--- System Pr&|
--- GemFire P&|
--- GemFireXD&

3 rows selected
```
