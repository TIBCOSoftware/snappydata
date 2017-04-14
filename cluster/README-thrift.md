## Introduction

SnappyData store now has support for Thrift protocol that provides functionality equivalent to
JDBC/ODBC protocols and can be used to access the store from other languages not yet supported
directly by SnappyData.  Compared to the Spark [thrift server](http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server) based on Hive, this has multiple advantages:

* Spark Hive thrift server provides a single point of entry that will spawn its own executors.
  In the SnappyData embedded mode, this means that only one "lead" node can act as a thrift-server
  at any point of time. The thrift server implementation in SnappyData has no such limitation
  that can be started on each of the executors.
* Enables writing a driver with implicit failover, high-availability characteristics.
  The SnappyData JDBC driver is based on thrift API which uses these characteristics of the API.
* Hive thrift API is quite limited and lacks a lot of functionality required for full
  JDBC/ODBC compliance. The thrift API supported by SnappyData store is rich enough to write
  a fully compliant JDBC/ODBC drivers including mutability, cursors etc.
* There is no open source ODBC driver available for Hive thrift server so that limits
  its usefulness in terms of overall tools connectivity (e.g. Tableau).


## Thrift server

The _-client-bind-address_ and _client-port_ arguments start a thrift server
by default in SnappyData product and a DRDA server in the old "rowstore" mode.
The defaults for these are _localhost_ and _1527_ respectively.
The _run-netserver_ option controls whether to start a network server or not (default is true).

The command-line SnappyData locators and servers accept _-thrift-server-address_
and _-thrift-server-port_ arguments to start a Thrift server in addition
to above. The thrift servers use the _Thrift Compact Protocol_ by default
which is not SSL enabled. When using the _snappy-start-all.sh_ script, these
properties can be specified in the conf/locators and conf/servers file in the
product directory like any other locator/server properties. For example, copy the
conf/locators.template and conf/servers.template files to conf/locators and conf/servers
respectively and add these arguments as required.

Add to conf/locators:
```
host1      -client-bind-address=host1 -client-port=1530
```

Provide appropriate values to _host1_ and the port 1530 above. If running SnappyData in the
_rowstore_ mode use thrift specific arguments like:

```
host1      -thrift-server-address=host1 -thrift-server-port=1530 -run-netserver=false
```

This also adds _run-netserver=false_ to inhibit starting the DRDA server or one can skip this
to also start the default DRDA server. If starting on localhost, then the
_client-bind-address_ and _thrift-server-address_ parameters can be skipped:

```
host1      -client-bind-address=host1 -client-port=1530
```

```
localhost  -thrift-server-port=1530 -run-netserver=false
```

Similarly add the above parameters to conf/servers.


Other optional startup Thrift properties include:

* _thrift-binary-protocol=(true|false)_: to use the thrift binary protocol instead of default compact protocol
* _thrift-framed-transport=(true|false)_: to use the thrift framed transport; this is not the
  recommended mode since it provides no advantages over the default with SnappyData's server
  implementation but has been provided for languages that only support framed transport
* _thrift-ssl=(true|false)_: enable SSL
* _thrift-ssl-properties_: comma-separated SSL properties including:
  * _protocol_: default "TLS", see [JCA docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#SSLContext)
  * _enabled-protocols_: enabled protocols separated by ":"
  * _cipher-suites_: enabled cipher suites separated by ":", see [JCA docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#ciphersuites)
  * _client-auth_=(true|false): if client also needs to be authenticated, see [J2SE docs](https://docs.oracle.com/javase/7/docs/api/javax/net/ssl/SSLServerSocket.html#setNeedClientAuth(boolean))
  * _keystore_: path to key store file
  * _keystore-type_: the type of key-store (default "JKS"), see [JCA docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#KeyStore)
  * _keystore-password_: password for the key store file
  * _keymanager-type_: the type of key manager factory, see [JSSE docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#KeyManagerFactory)
  * _truststore_: path to trust store file
  * _truststore-type_: the type of trust-store (default "JKS"), see [JCA docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/StandardNames.html#KeyStore)
  * _truststore-password_: password for the trust store file
  * _trustmanager-type_: the type of trust manager factory, see [JSSE docs](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#TrustManagerFactory)


If using these properties, then the client has to setup the corresponding Thrift client socket
options as per the programming language being used. For instance if _thrift-binary-protocol_
is set as true on server, then a java client will need to use _TBinaryProtocol_ from the
Thrift Java API for the protocol.


## Thrift client

The current thrift IDL file can be found [here](https://github.com/SnappyDataInc/snappy-store/blob/snappy/master/gemfirexd/shared/src/main/java/io/snappydata/thrift/common/snappydata.thrift).
Client drivers can be generated from the IDL as described in its [documentation](https://thrift.apache.org/).
A description of the thrift types etc can also be found there and [elsewhere](https://diwakergupta.github.io/thrift-missing-guide/).

The _SnappyDataService_ provides the full set of operations that can be performed on the servers.
Basic steps are given below. A complete Java Thrift client example can be found [here](https://github.com/SnappyDataInc/snappy-store/blob/snappy/master/gemfirexd/tools/src/test/java/io/snappydata/app/TestThrift.java).


### Open a connection

This includes creating a Thrift socket with appropriate protocol, then invoking
the _openConnection_ API providing appropriate _OpenConnectionArgs_.
Three parameters that need to be provided are:
* _clientHostName_: the host name of the client used for mapping on the server
* _clientID_: a unique ID for the connection that can be used in logs, maps etc on the server;
              this must be unique on that _clientHostName_
* _security_: the security mechanism to use, currently only SecurityMechanism.PLAIN is supported

No userName or password arguments have been provided in the example below assuming
no authentication has been configured by default on the locators/servers.
For the clientID, some symbolic name for the client followed by the threadId have been used.

```java
  import io.snappydata.thrift.*;
  import org.apache.thrift.*;
  import org.apache.thrift.protocol.*;
  import org.apache.thrift.transport.*;


  String myHostName = InetAddress.getLocalHost().getCanonicalHostName();
  TSocket socket = new TSocket("localhost", 1531);
  TCompactProtocol inProtocol = new TCompactProtocol(socket);
  TCompactProtocol outProtocol = new TCompactProtocol(socket);
  socket.open();

  Thread currentThread = Thread.currentThread();
  OpenConnectionArgs connArgs = new OpenConnectionArgs()
      .setClientHostName(myHostName)
      .setClientID("javaClient1|0x" + Long.toHexString(currentThread.getId()))
      .setSecurity(SecurityMechanism.PLAIN);
  SnappyDataService.Client conn = new SnappyDataService.Client(
      inProtocol, outProtocol);
  ConnectionProperties connProperties = conn.openConnection(connArgs);
```

The returned _ConnectionProperties_ contains a long connection ID and a unique token
to use for subsequent Thrift API calls.


### Execute a statement

The _connectionId_ and _token_ fields of the _ConnectionProperties_ provide a connection oriented
feel to the rest of the API which requires one or both of these to be passed to nearly all other
API calls. The calls can be invoked using any underlying thrift connection passing them.
A simple (un-prepared) SQL statement execution can be done using APIs like execute/executeUpdate.

```java
  long connId = conn.getId();
  ByteBuffer token = conn.getToken();
  conn.execute(connId, "create table foo (bar int primary key)",
      null, null, token);
```

Or a statement with update count:

```java
  StatementResult result = conn.execute(connId, "insert into foo values (1), (2)",
      null, null, token);
  if (result.updateCount != 2) {
    throw new AssertionError("Expected update count to be 2 but was " + result.updateCount);
  }
```

### Prepared statement

Like the JDBC/ODBC, the Thrift API allows "preparing" a statement having placeholders
that can be re-used across multiple executions with different parameters.
The _prepareStatement_ and _prepareAndExecute_ APIs allow one to prepare a statement
with latter also executing it the first time. The result of prepare will provide the
meta-data of the parameters which can be used to also fill in the _Row_ having parameters.

```java
  PrepareResult pstmt = conn.prepareStatement(connId,
      "insert into foo values (?)", null, null, token);
  Row params = new Row(pstmt.getParameterMetaData());
  int count;

  for (int bar = 1; bar <= 10; bar++) {
    params.setInt(0, bar);
    count = conn.executePreparedUpdate(pstmt.statementId,
        params, null, token).updateCount;
    if (count != 1) {
      throw new AssertionError("Unexpected count for single insert = " + count);
    }
  }
```

### Handling result sets

Queries returning result sets provide a _RowSet_ object that will have the meta-data, data,
warnings etc. The result data itself is provided as a list of _Row_ objects that can be
iterated using the normal language-specific list iteration. An example in java can look like:

```java

  pstmt = conn.prepareStatement(connId, "select * from foo where bar=?",
      null, null, token);
  params = new Row(pstmt.getParameterMetaData());

  RowSet rs;
  for (int bar = 1; bar <= 5; bar++) {
    params.setInt(0, bar);

    rs = conn.executePreparedQuery(pstmt.statementId, params, null, token);

    int numResults = 0;
    for (Row row : rs.getRows()) {
      System.out.println("For bar=" + bar + " select result = " + row.getInt(1));
      numResults++;
    }
  }
```

### Close the connection

Clients should invoke both the _closeConnection_ API (that will clear server-side
    artificats for the connection) as well as close the client socket.

```java
  conn.closeConnection(connId, token);
  conn.getInputProtocol().getTransport().close();
```

### Handling failover

The API defines a separate _LocatorService_ for locators (the methods in that service are
    also available on _SnappyDataService_ on servers) that provides a _getPreferredServer_
method to discover the _preferred_ server as per load. In case of a _TTransportException_
on a client connection, clients can invoke the _getPreferredServer_ again providing the
failed server(s) as argument to get a new server to use. In addition when retrying a statement
execution after failover, set the _possibleDuplicate_ flag on the _StatementAttributes_.
The java example below only demonstrates how to use the locator connection (which can be
    one connection per client) to discover the _preferred_ server to use instead of
hard-coding the server host/port.

```java
  // search only for servers using TCompactProtocol without SSL
  Set<ServerType> serverType = Collections.singleton(ServerType.THRIFT_SNAPPY_CP);
  TSocket socket = new TSocket("localhost", 1530);
  TCompactProtocol inProtocol = new TCompactProtocol(socket);
  TCompactProtocol outProtocol = new TCompactProtocol(socket);
  socket.open();
  LocatorService.Client controlService = new LocatorService.Client(
      inProtocol, outProtocol);

  HostAddress preferredServer = controlService.getPreferredServer(
      serverType, null, null);

  System.out.println("Attempting connection to preferred server:port = " +
      preferredServer.getHostName() + ':' + preferredServer.getPort());

  TSocket socket = new TSocket(preferredServer.getHostName(),
      preferredServer.getPort());
  TCompactProtocol inProtocol = new TCompactProtocol(socket);
  TCompactProtocol outProtocol = new TCompactProtocol(socket);
  socket.open();

  Thread currentThread = Thread.currentThread();
  OpenConnectionArgs connArgs = new OpenConnectionArgs()
      .setClientHostName(myHostName)
      .setClientID("javaClient1|0x" + Long.toHexString(currentThread.getId()))
      .setSecurity(SecurityMechanism.PLAIN);
  SnappyDataService.Client conn = new SnappyDataService.Client(
      inProtocol, outProtocol);
  ConnectionProperties connProperties = conn.openConnection(connArgs);
```
