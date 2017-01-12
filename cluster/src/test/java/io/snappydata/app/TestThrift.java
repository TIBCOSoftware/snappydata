/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.app;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.internal.shared.NativeCalls;
import io.snappydata.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;

public class TestThrift {

  public static void main(String[] args) throws Exception {
    String locatorHost = "localhost";
    int locatorPort = 1530;

    if (args.length > 0) {
      locatorHost = args[0];
      if (args.length > 1) {
        try {
          locatorPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException nfe) {
          System.err.println(nfe.toString());
          System.err.println(
              "Usage: java -cp ... TestThrift [<locator-host> <locator-port>]");
          System.exit(1);
        }
      }
    }

    String hostName = InetAddress.getLocalHost().getCanonicalHostName();
    int pid = NativeCalls.getInstance().getProcessId();

    // search only for servers using TCompactProtocol without SSL
    Set<ServerType> serverType = Collections.singleton(ServerType.THRIFT_SNAPPY_CP);
    TSocket socket = new TSocket(locatorHost, locatorPort);
    TCompactProtocol inProtocol = new TCompactProtocol(socket);
    TCompactProtocol outProtocol = new TCompactProtocol(socket);
    socket.open();
    LocatorService.Client controlService = new LocatorService.Client(
        inProtocol, outProtocol);

    ClientConnection conn = ClientConnection.open(hostName, pid,
        controlService, serverType);

    // create the table
    ByteBuffer token = conn.getToken();
    conn.execute(conn.getId(), "drop table if exists orders",
        null, null, token);
    conn.execute(conn.getId(), "create table orders (" +
        "no_w_id  integer   not null," +
        "no_d_id  integer   not null," +
        "no_o_id  integer   not null," +
        "no_name  varchar(100) not null" +
        ") partition by (no_w_id)", null, null, token);

    System.out.println("Created table");

    final int numRows = 10000;
    PrepareResult pstmt = conn.prepareStatement(conn.getId(),
        "insert into orders values (?, ?, ?, ?)", null, null, token);

    System.out.println("Starting inserts");

    int id, w_id, count;
    String name;
    Row params = new Row(pstmt.getParameterMetaData());
    for (id = 1; id <= numRows; id++) {
      w_id = (id % 98);
      name = "customer-with-order" + id + '_' + w_id;

      params.setInt(0, w_id);
      params.setInt(1, w_id);
      params.setInt(2, id);
      params.setObject(3, name, SnappyType.VARCHAR);

      count = conn.executePreparedUpdate(pstmt.statementId,
          params, token).updateCount;
      if (count != 1) {
        System.err.println("Unexpected count for single insert: " + count);
        System.exit(2);
      }
      if ((id % 500) == 0) {
        System.out.println("Completed " + id + " inserts ...");
      }
    }

    pstmt = conn.prepareStatement(conn.getId(), "SELECT * FROM orders " +
            "WHERE no_d_id = ? AND no_w_id = ? AND no_o_id = ?",
        null, null, token);
    params = new Row(pstmt.getParameterMetaData());

    final int numRuns = 50000;
    int rowNum;
    RowSet rs;
    StatementResult sr;

    // first round is warmup for the selects
    for (int runNo = 1; runNo <= 2; runNo++) {
      long start = System.currentTimeMillis();
      if (runNo == 1) {
        System.out.println("Starting warmup selects");
      } else {
        System.out.println("Starting timed selects");
      }
      for (int i = 1; i <= numRuns; i++) {
        rowNum = (i % numRows) + 1;
        w_id = (rowNum % 98);

        params.setInt(0, w_id);
        params.setInt(1, w_id);
        params.setInt(2, rowNum);

        sr = conn.executePrepared(pstmt.statementId, params,
            Collections.<Integer, OutputParameter>emptyMap(), token);
        // rs = conn.executePreparedQuery(pstmt.statementId, params, token);
        rs = sr.getResultSet();

        int numResults = 0;
        for (Row row : rs.getRows()) {
          row.getInt(1);
          row.getObject(3);
          numResults++;
        }
        if (numResults == 0) {
          System.err.println("Unexpected 0 results for w_id,d_id = " + w_id);
          System.exit(2);
        }
        if (runNo == 1 && (i % 500) == 0) {
          System.out.println("Completed " + i + " warmup selects ...");
        }
      }
      long end = System.currentTimeMillis();

      if (runNo == 2) {
        System.out.println("Time taken for " + numRuns + " selects: " +
            (end - start) + "ms");
      }
    }

    conn.execute(conn.getId(), "drop table orders", null, null, token);
    conn.close();

    controlService.closeConnection();
    socket.close();
  }

  static class ClientConnection extends SnappyDataService.Client {

    ConnectionProperties connProperties;

    private ClientConnection(TCompactProtocol inProtocol,
        TCompactProtocol outProtocol) {
      super(inProtocol, outProtocol);
    }

    public long getId() {
      return this.connProperties.connId;
    }

    public ByteBuffer getToken() {
      return this.connProperties.token;
    }

    static ClientConnection open(String hostName, int pid,
        LocatorService.Client controlService,
        Set<ServerType> serverType) throws TException {
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
          .setClientHostName(hostName)
          .setClientID(pid + "|0x" + Long.toHexString(currentThread.getId()))
          .setSecurity(SecurityMechanism.PLAIN);
      ClientConnection connection = new ClientConnection(
          inProtocol, outProtocol);
      connection.connProperties = connection.openConnection(connArgs);
      return connection;
    }

    void close() throws TException {
      closeConnection(connProperties.connId, connProperties.token);
      getInputProtocol().getTransport().close();
    }
  }
}
