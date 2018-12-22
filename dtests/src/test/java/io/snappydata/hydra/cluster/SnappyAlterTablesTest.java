/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package io.snappydata.hydra.cluster;

import util.TestException;

import java.sql.Connection;
import java.sql.SQLException;

public class SnappyAlterTablesTest extends SnappyTest {

  public static void HydraTask_alterTables() throws SQLException {
    String query = null;
    Connection conn = getLocatorConnection();
    query = "ALTER TABLE orders ADD FirstName String";
    executeUpdate(conn, query);
    query = "SELECT count(*) FROM orders";
    executeQuery(conn, query);
    query = "insert into app.orders values(10251,'AROUT',4,'1996-07-08 00:00:00.000'," +
        "'1996-08-05 00:00:00.000','1996-07-15 00:00:00.000',4,41.34," +
        "'Victuailles en stock 2','rue du Commerce','Lyon',NULL,'69004','France'," +
        "'empFirstName2')";
    executeUpdate(conn, query);
    query = "insert into app.orders values(10260,'CENTC',13,'1996-07-19 00:00:00.000'," +
        "'1996-08-16 00:00:00.000','1996-07-29 00:00:00.000',13,55.09,'Ottilies Kaoseladen'," +
        "'Mehrheimerstr. 369','Kaqln',NULL,'50739','Germany','empFirstName4')";
    executeUpdate(conn, query);
    query = "insert into app.orders values(10265,'DUMON',18,'1996-07-25 00:00:00.000'," +
        "'1996-08-22 00:00:00.000','1996-08-12 00:00:00.000',18,55.28," +
        "'Blondel pare et fils 24', 'place Klacber','Strasbourg',NULL," +
        "'67000','France','empFirstName5')";
    executeUpdate(conn, query);
    query = "SELECT * FROM orders where FirstName='empFirstName2'";
    executeQuery(conn, query);
    query = "SELECT * FROM orders where FirstName='empFirstName4'";
    executeQuery(conn, query);
    query = "SELECT * FROM orders where FirstName='empFirstName5'";
    executeQuery(conn, query);
    query = "delete from orders where FirstName='empFirstName2'";
    executeUpdate(conn, query);
    query = "delete from orders where FirstName='empFirstName4'";
    executeUpdate(conn, query);
    query = "delete from orders where FirstName='empFirstName5'";
    executeUpdate(conn, query);
    query = "SELECT * FROM orders where FirstName='empFirstName5'";
    executeQuery(conn, query);
    query = "ALTER TABLE order_details ADD CustomerID String";
    executeUpdate(conn, query);
    query = "SELECT count(*) FROM order_details";
    executeQuery(conn, query);
    query = "insert into app.order_details values(10250,72,34.8,5,0,'custID4')";
    executeUpdate(conn, query);
    query = "insert into app.order_details values(10249,42,9.8,10,0,'custID14')";
    executeUpdate(conn, query);
    query = "insert into app.order_details values(10253,41,7.7,10,0,'custID7')";
    executeUpdate(conn, query);
    query = "SELECT * FROM order_details where CustomerID='custID4'";
    executeQuery(conn, query);
    query = "SELECT * FROM order_details where CustomerID='custID14'";
    executeQuery(conn, query);
    query = "SELECT * FROM order_details where CustomerID='custID7'";
    executeQuery(conn, query);
    query = "delete from order_details where CustomerID='custID4'";
    executeUpdate(conn, query);
    query = "delete from order_details where CustomerID='custID14'";
    executeUpdate(conn, query);
    query = "delete from order_details where CustomerID='custID7'";
    executeUpdate(conn, query);
    query = "SELECT * FROM order_details where CustomerID='custID14'";
    executeQuery(conn, query);
    query = "ALTER TABLE orders DROP COLUMN FirstName";
    executeUpdate(conn, query);
    query = "ALTER TABLE order_details DROP COLUMN CustomerID";
    executeUpdate(conn, query);
    query = "SELECT count(*) FROM orders";
    executeQuery(conn, query);
    query = "SELECT count(*) FROM order_details";
    executeQuery(conn, query);
    closeConnection(conn);
  }

  protected static void executeQuery(Connection conn, String query) {
    try {
      conn.createStatement().executeQuery(query);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing query:" + query, se);
    }
  }

  protected static void executeUpdate(Connection conn, String query) {
    try {
      conn.createStatement().executeUpdate(query);
    } catch (SQLException se) {
      throw new TestException("Got exception while executing query:" + query, se);
    }
  }
}

