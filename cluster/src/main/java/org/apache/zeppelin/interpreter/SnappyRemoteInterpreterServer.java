package org.apache.zeppelin.interpreter;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer;

/**
 * Created by sachin on 3/6/16.
 */
public class SnappyRemoteInterpreterServer extends RemoteInterpreterServer {
  public SnappyRemoteInterpreterServer(int port) throws TTransportException {
    super(port);
  }

  //Overriding the RemoteInterpreterServer's Shutdown method to avoid shutdown of the Leadnode and RemoteInterpreter by zeppelin server
  @Override
  public void shutdown() throws TException {

  }
}
