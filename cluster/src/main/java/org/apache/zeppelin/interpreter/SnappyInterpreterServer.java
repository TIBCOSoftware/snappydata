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
package org.apache.zeppelin.interpreter;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer;

/**
 *
 */
public class SnappyInterpreterServer extends RemoteInterpreterServer {
  public SnappyInterpreterServer(int port) throws TTransportException {
    super(port);
  }

  //Overriding the RemoteInterpreterServer's Shutdown method to avoid shutdown of the Leadnode and RemoteInterpreter by zeppelin server
  @Override
  public void shutdown() throws TException {

  }
}
