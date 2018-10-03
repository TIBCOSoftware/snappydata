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

import org.apache.spark.sql.streaming.jdbc.SourceSpec;
import scala.Option;
import scala.Some;

public class SqlServerSpec implements SourceSpec {

  @Override
  public String offsetColumn() {
    return "__$start_lsn";
  }

  @Override
  public Option<String> offsetToStrFunc() {
    return new Some<>("master.dbo.fn_varbintohexstr");
  }

  @Override
  public Option<String> strToOffsetFunc() {
    return new Some<>("master.dbo.fn_cdc_hexstrtobin");
  }

  @Override
  public String projectedColumns(){
    String offsetColumn = offsetColumn();
    String query = String.format("master.dbo.fn_varbintohexstr(%s) as STRLSN," +
            "sys.fn_cdc_map_lsn_to_time(%s) as LSNTOTIME, *", offsetColumn, offsetColumn);
    return query;
  }

  @Override
  public String getNextOffset(String tableName, String currentOffset, int maxEvents) {
    String offsetColumn = offsetColumn();
    String query = String.format("select master.dbo.fn_varbintohexstr(max(%s)) nextLSN " +
                    "from (select %s, rank() over " +
                    "(order by %s) runningCount " +
                    "from %s where %s > master.dbo.fn_cdc_hexstrtobin('%s') " +
                    " ) x where runningCount <= %d", offsetColumn, offsetColumn, offsetColumn,
            tableName, offsetColumn, currentOffset, maxEvents);
    return query;
  }
}