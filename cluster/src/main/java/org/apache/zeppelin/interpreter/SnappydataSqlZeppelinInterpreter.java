/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.interpreter;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.jdbc.JDBCInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snappydatasql interpreter used to connect snappydata cluster using jdbc for performing sql
 * queries
 * 
 * @author sachin
 *
 */
public class SnappydataSqlZeppelinInterpreter extends JDBCInterpreter {
  static volatile boolean firstTime = true;
  private static BlockingQueue<InterpreterContextRunner> queue =
      new ArrayBlockingQueue<InterpreterContextRunner>(1);
  private Logger logger = LoggerFactory.getLogger(SnappydataSqlZeppelinInterpreter.class);

  public SnappydataSqlZeppelinInterpreter(Properties property) {
    super(property);

    new Thread() {
      public void run() {
        while (true) {
          InterpreterContextRunner runner;
          try {
            runner = queue.take();
            Thread.sleep(400);
            runner.run();
          } catch (InterruptedException e) {
            logger.error(
                "Error while scheduling the approx query.Actual Exception " + e.getMessage());
          }
        }
      };
    }.start();

  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    String id = contextInterpreter.getParagraphId();
    logger.info(id + " FirstTime = " + firstTime + " this " + this.hashCode() + " scheduler "
        + getScheduler(), new Exception("SJ:"));

    if (cmd.trim().startsWith("approxResultFirst=true")) {
      cmd = cmd.replaceFirst("approxResultFirst=true", "");
      if (firstTime) {
        firstTime = false;

        for (InterpreterContextRunner r : contextInterpreter.getRunners()) {
          if (id.equals(r.getParagraphId())) {


            logger.info("Executing cmd before " + cmd);
            final InterpreterResult res = super.interpret(cmd, contextInterpreter);
            logger.info("Executing cmd after  schedule ");

            queue.add(r);

            logger.info("Some delay before returning orig query result");

            return res;
          }
        }

      } else {
        firstTime = true;
        logger.info("Before delay ");
        /*
         * try { Thread.sleep(10000); } catch (InterruptedException e) { e.printStackTrace(); }
         */
        String query = cmd.replaceAll("with error .*", "");
        logger.info("Executing cmd " + query);
        // InterpreterResult res =

        /*
         * contextInterpreter.getAngularObjectRegistry().add("title", "Accurate result",
         * contextInterpreter.getNoteId(), contextInterpreter.getParagraphId());
         */
        return super.interpret(query, contextInterpreter);
      }
      return null;
    } else {
      return super.interpret(cmd, contextInterpreter);

    }


  }

}
