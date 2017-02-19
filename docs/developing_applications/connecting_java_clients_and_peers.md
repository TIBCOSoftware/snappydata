#Connecting Java Clients and Peers
A Java application can use the JDBC thin client driver to connect to a SnappyData cluster and execute SQL statements. Or, a Java application can use the JDBC peer client driver to boot a SnappyData peer process and participate as a member of the cluster.

Both drivers use the basic JDBC connection URL of `jdbc:snappydata:` where:

 * `jdbc`: is the protocol.

 * `snappydata`: is the subprotocol.

Thin clients provide a light weight JDBC connection to the distributed system. By default, a thin client connects to an existing SnappyData data store member, which acts as the transaction coordinator for executing distributed transactions. This provides either one-hop or two-hop access for executing queries or DML statements, depending on where data is located in the system. For certain types of queries, you can enable single-hop access for thin-client connections to improve query performance. See Enabling Single-Hop Data Access.

<mark>Check with Shirish</mark>
 * **Thin Client JDBC Driver Connections** The thin client driver class is packaged in com.pivotal.snappydata.jdbc.ClientDriver. In addition to the basic JDBC Connection URL, you specify the host and port number of a SnappyData server or a locator to which the thin client driver will connect.