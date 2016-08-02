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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.jdbc.JDBCInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.StringUtils.containsIgnoreCase;

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
  static final String DEFAULT_KEY = "default";

  private static final char WHITESPACE = ' ';
  private static final char NEWLINE = '\n';
  private static final char TAB = '\t';
  private static final String TABLE_MAGIC_TAG = "%table ";
  private static final String EXPLAIN_PREDICATE = "EXPLAIN ";
  private static final String UPDATE_COUNT_HEADER = "Update Count";


  static final String EMPTY_COLUMN_VALUE = "";


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
    String propertyKey = getPropertyKey(cmd);
    if (null != propertyKey && !propertyKey.equals(DEFAULT_KEY)) {
      cmd = cmd.substring(propertyKey.length() + 2);
    }
    cmd = cmd.trim();
    if (cmd.trim().startsWith("approxResultFirst=true")) {
      cmd = cmd.replaceFirst("approxResultFirst=true", "");


      if (firstTime) {
        firstTime = false;

        for (InterpreterContextRunner r : contextInterpreter.getRunners()) {
          if (id.equals(r.getParagraphId())) {


            final InterpreterResult res = executeSql(propertyKey, cmd, contextInterpreter);

            queue.add(r);

            return res;
          }
        }

      } else {
        firstTime = true;
        /*
         * try { Thread.sleep(10000); } catch (InterruptedException e) { e.printStackTrace(); }
         */
        String query = cmd.replaceAll("with error .*", "");
        logger.info("Executing cmd " + query);

        /*
         * contextInterpreter.getAngularObjectRegistry().add("title", "Accurate result",
         * contextInterpreter.getNoteId(), contextInterpreter.getParagraphId());
         */
        return executeSql(propertyKey, cmd, contextInterpreter);
      }
      return null;
    } else {
      return executeSql(propertyKey, cmd, contextInterpreter);

    }


  }

  /**
   * The content of this method are borrowed from JDBC interpreter of apache zeppelin
   * @param propertyKey
   * @param sql
   * @param interpreterContext
   * @return
   */
  private InterpreterResult executeSql(String propertyKey, String sql,
      InterpreterContext interpreterContext) {

    String userId = interpreterContext.getAuthenticationInfo().getUser();

    try {

      Statement statement = getStatement(propertyKey, userId);

      if (statement == null) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, "Prefix not found.");
      }
      statement.setMaxRows(getMaxResult());

      StringBuilder msg = null;
      boolean isTableType = false;

      if (containsIgnoreCase(sql, EXPLAIN_PREDICATE)) {
        msg = new StringBuilder();
      } else {
        msg = new StringBuilder(TABLE_MAGIC_TAG);
        isTableType = true;
      }

      ResultSet resultSet = null;
      try {

        boolean isResultSetAvailable = statement.execute(sql);

        if (isResultSetAvailable) {
          resultSet = statement.getResultSet();

          ResultSetMetaData md = resultSet.getMetaData();

          for (int i = 1; i < md.getColumnCount() + 1; i++) {
            if (i > 1) {
              msg.append(TAB);
            }
            msg.append(replaceReservedChars(isTableType, md.getColumnName(i)));
          }
          msg.append(NEWLINE);

          int displayRowCount = 0;
          while (resultSet.next() && displayRowCount < getMaxResult()) {
            for (int i = 1; i < md.getColumnCount() + 1; i++) {
              Object resultObject;
              String resultValue;
              resultObject = resultSet.getObject(i);
              if (resultObject == null) {
                resultValue = "null";
              } else {
                resultValue = resultSet.getString(i);
              }
              msg.append(replaceReservedChars(isTableType, resultValue));
              if (i != md.getColumnCount()) {
                msg.append(TAB);
              }
            }
            msg.append(NEWLINE);
            displayRowCount++;
          }
        } else {
          // Response contains either an update count or there are no results.
          int updateCount = statement.getUpdateCount();
          msg.append(UPDATE_COUNT_HEADER).append(NEWLINE);
          msg.append(updateCount).append(NEWLINE);
        }
      } finally {
        try {
          if (resultSet != null) {
            resultSet.close();
          }
          statement.close();
        } finally {
          statement = null;
        }
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, msg.toString());

    } catch (Exception e) {
      logger.error("Cannot run " + sql, e);
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(e.getMessage()).append("\n");
      stringBuilder.append(e.getClass().toString()).append("\n");
      stringBuilder.append(StringUtils.join(e.getStackTrace(), "\n"));
      return new InterpreterResult(InterpreterResult.Code.ERROR, stringBuilder.toString());
    }
  }


  /**
   * This method is borrowed from JDBC interpreter of apache zeppelin
   * For %table response replace Tab and Newline characters from the content.
   */
  private String replaceReservedChars(boolean isTableResponseType, String str) {
    if (str == null) {
      return EMPTY_COLUMN_VALUE;
    }
    return (!isTableResponseType) ? str : str.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE);
  }
}
