# MEMBERS

A RowStore virtual table that contains information about each distributed system member.

See <a href="../../manage_guide/Topics/distributed-sysadmin/member-info.html#concept_2FFD239F66BD4A0099E401F1FC250574" class="xref" title="The SYS.MEMBERS table provides information about all peers and servers that make up a RowStore distributed system. You can use different queries to obtain details about individual members and their role in the cluster.">Distributed System Membership Information</a>. <p 

	!!! Note
		RowStore converts server group names to all-uppercase letters before storing the values in the SYS.MEMBERS table. DDL statements and procedures automatically convert any supplied server group values to all-uppercase letters. However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.

<a id="reference_21873F7CB0454C4DBFDC7B4EDADB6E1F__table_F5B916925318472FB3BB0B850DBBA41F"></a>

|Column Name|Type |Length |Nullable|Contents|
| ------------ | ------------- | ------------ | ------------ | ------------- |
|ID|VARCHAR|128|No|The unique ID of the member. This ID has the format: <br>`hostname(process_id)&lt;member_number&gt;:udp_port/tcp_port`<br>For example:<br>10.0.1.31(66878)&lt;v0&gt;:41715/63386|
|KIND  |VARCHAR   | 24 |No   | Specifies the type of RowStore member process: <br> * datastore—A member that hosts data.<br> * peer—A member that does not host data.<br> * locator—Provides discovery services for a cluster.<br> Member types can also be qualified with additional keywords <br>  * normal—The member can communicate with other members in a cluster. <br> * loner—The member is standalone and cannot communicate with other members. Loners are started with mcast-port=0 and use no locators for discovery.<br> * admin—The member also acts as a JMX manager node. |
|HOSTDATA  | BOOLEAN  | | Yes  |A value of ‘1’ indicates that this member is a data store and can host data. Otherwise, the member is a peer client with no hosted data. |	
|ISELDER  | BOOLEAN  |  |No |	Is this the eldest member of the distributed system. Typically, this is the member who first joins the cluster.|
|IPADDRESS  |  VARCHAR |64  |   Yes| The fully-qualified hostname/IP address of the member.|
|HOST   |VARCHAR  | 128  | Yes | The fully-qualified hostname of the member.|
| PID  |INTEGER  |10   | No |The member process ID. |
|PORT   |INTEGER  | 10  | No | The member UDP port.|
|ROLES   |VARCHAR  |128 | No |Not used. |
|NETSERVERS   |VARCHAR  |32672|No  |Host and port information for Network Servers that are running on RowStore members. |
|LOCATOR   |VARCHAR  |32672 | No |Host and port information for locator members. |
|SERVERGROUPS   |VARCHAR  | 32672 |No |A comma-separated list of server groups of which this member is a part. <br> **Note**: RowStore converts server group names to all-uppercase letters before storing the values in the SYS.MEMBERS table. DDL statements and procedures automatically convert any supplied server group values to all-uppercase letters. However, you must specify uppercase values for server groups when you directly query the SYS.MEMBERS table.|
|SYSTEMPROPS   |CLOB  | 2147483647  |No  | A list of all system properties used to start this member. This includes properties such as the classpath, JVM version, and so forth.|	
|GEMFIREPROPS   |CLOB  |2147483647   | No  |The names and values of GemFire core system properties that the member uses. See [Configuration Properties](http://rowstore.docs.snappydata.io/docs/reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes) for property descriptions.|	
|BOOTPROPS   |CLOB  |2147483647   | No |All of the RowStore boot properties names and values that a member uses. See [Configuration Properties](http://rowstore.docs.snappydata.io/docs/reference/configuration/ConnectionAttributes.html#jdbc_connection_attributes) for property descriptions. |	

: <span class="tablecap">Table 1. MEMBERS system table</span>


