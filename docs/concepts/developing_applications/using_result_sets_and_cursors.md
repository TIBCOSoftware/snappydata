##Using Result Sets and Cursors
A result set maintains a cursor, which points to its current row of data. You can use a result set to step through and process the rows one by one.

In SnappyData, any SELECT statement generates a cursor that can be controlled using a `java.sql.ResultSet` object. SnappyData does not support SQL-92’s DECLARE CURSOR language construct to create cursors, but it does support positioned deletes and positioned updates with updatable cursors.

!!! Note: 
    This topic was adapted from the Apache Derby documentation source, and is subject to the Apache license: “ pre Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. “

<mark>Check with Shirish</mark>

* **Non-updatable, Forward-Only Result Sets**: The simplest result set that SnappyData supports cannot be updated, and has a cursor that moves only one way, forward.
* **Updatable Result Sets**: You update result sets in SnappyData by using result set update methods (updateRow(),deleteRow() and insertRow()), or by using positioned update or delete queries. SnappyData supports updatable result sets that are both scrollable and non-scrollable (forward-only).
* **Scrollable Insensitive Result Sets**: JDBC provides two types of result sets that allow you to scroll in either direction or to move the cursor to a particular row. SnappyData supports one of these types: scrollable insensitive result sets (ResultSet.TYPE_SCROLL_INSENSITIVE).
* **Result Sets and Autocommit**: Issuing a commit causes all result sets on your connection to be closed.
