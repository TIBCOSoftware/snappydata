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
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.jdbc.JDBCInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.StringUtils.containsIgnoreCase;

/**
 * Snappydatasql interpreter used to connect snappydata cluster using jdbc for performing sql
 * queries
 *
 */
public class SnappyDataSqlZeppelinInterpreter extends JDBCInterpreter {
  public static final String SHOW_APPROX_RESULTS_FIRST = "show-approx-results-first";
  static volatile boolean firstTime = true;
  private Logger logger = LoggerFactory.getLogger(SnappyDataSqlZeppelinInterpreter.class);
  static final String DEFAULT_KEY = "default";

  private static final char WHITESPACE = ' ';
  private static final char NEWLINE = '\n';
  private static final char TAB = '\t';
  private static final String TABLE_MAGIC_TAG = "%table ";
  private static final String EXPLAIN_PREDICATE = "EXPLAIN ";
  private static final String UPDATE_COUNT_HEADER = "Update Count";
  private static final ExecutorService exService = Executors.newSingleThreadExecutor();
  static final String EMPTY_COLUMN_VALUE = "";


  public SnappyDataSqlZeppelinInterpreter(Properties property) {
    super(property);
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    String id = contextInterpreter.getParagraphId();
    String propertyKey = getPropertyKey(cmd);
    if (null != propertyKey && !propertyKey.equals(DEFAULT_KEY)) {
      cmd = cmd.substring(propertyKey.length() + 2);
    }
    cmd = cmd.trim();


    if (cmd.startsWith(SHOW_APPROX_RESULTS_FIRST)) {
      cmd = cmd.replaceFirst(SHOW_APPROX_RESULTS_FIRST, "");

      /**
       * As suggested by Jags and suyog
       * This will allow user to execute multiple queries in same paragraph.
       * But will return results of the last query.This is mainly done to
       * allow user to set properties for JDBC connection
       *
       */
      String queries[] = cmd.split(";");
      for (int i = 0; i < queries.length - 1; i++) {
        InterpreterResult result = executeSql(propertyKey, queries[i], contextInterpreter);
        if (result.code().equals(InterpreterResult.Code.ERROR)) {
          return result;
        }
      }
      if (firstTime) {
        firstTime = false;

        for (InterpreterContextRunner r : contextInterpreter.getRunners()) {
          if (id.equals(r.getParagraphId())) {

            String query = queries[queries.length - 1]+" with error";
            final InterpreterResult res = executeSql(propertyKey, query, contextInterpreter);

            try{
              /*Adding a delay of few milliseconds in order for zeppelin to render
               the result.As this delay is after the query execution it will not
               be considered in query time. This delay is basically the gap between
               first query and spawning of the next query.
              */
              Thread.sleep(200);
            } catch (InterruptedException interruptedException) {

              //Ignore this exception as this should not harm the behaviour
            }

            exService.submit(new QueryExecutor(r));
            return res;
          }
        }

      } else {
        firstTime = true;
        String query = queries[queries.length - 1];//.replaceAll("with error .*", "");


        return executeSql(propertyKey, query, contextInterpreter);
      }
      return null;
    } else {
      String queries[] = cmd.split(";");
      for (int i = 0; i < queries.length - 1; i++) {
        InterpreterResult result = executeSql(propertyKey, queries[i], contextInterpreter);
        if (result.code().equals(InterpreterResult.Code.ERROR)) {
          return result;
        }
      }
      return executeSql(propertyKey, queries[queries.length - 1], contextInterpreter);

    }


  }

  /**
   * The content of this method are borrowed from JDBC interpreter of apache zeppelin
   *
   * @param propertyKey
   * @param sql
   * @param interpreterContext
   * @return
   */
  private InterpreterResult executeSql(String propertyKey, String sql,
      InterpreterContext interpreterContext) {

    String paragraphId = interpreterContext.getParagraphId();

    try {

      //Getting connection per user instead of per paragraph
      Statement statement = getStatement(propertyKey, paragraphId);

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

          int maxResult = getMaxResult();
          int columnCount = md.getColumnCount();
          while (resultSet.next() && displayRowCount < maxResult) {
            for (int i = 1; i < columnCount + 1; i++) {
              Object resultObject;
              String resultValue;
              resultObject = resultSet.getObject(i);
              if (resultObject == null) {
                resultValue = "null";
              } else {
                resultValue = resultSet.getString(i);
              }
              msg.append(replaceReservedChars(isTableType, resultValue));
              if (i != columnCount) {
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


  class QueryExecutor implements Callable<Integer> {

    InterpreterContextRunner runner;
    QueryExecutor(InterpreterContextRunner runner){
      this.runner=runner;
    }
    @Override
    public Integer call() throws Exception {

      runner.run();

      return 0;
    }
  }
}
