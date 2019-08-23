/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package io.snappydata.hydra.alterTable

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField}

import scala.util.{Failure, Success, Try}

class AlterTablesJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("AlterTablesJob.out"), true));
    Try {

      val snc = snSession.sqlContext
      // scalastyle:off println
      pw.println(s"Alter tables Test started at : " + System.currentTimeMillis)
    //  snc.setSchema("gemfire1")
      snc.alterTable("orders", true, StructField("FirstName", StringType, true))
      var query = "SELECT * FROM orders"
      var result = snc.sql(query).schema.fields.length
      pw.println("schema fields length for orders" + "\nResults : " + result)
      assert(snc.sql("SELECT * FROM " + "orders").schema.fields.length == 15)
      snc.sql("insert into orders values(10251,'AROUT',4,'1996-07-08 00:00:00.000'," +
          "'1996-08-05 00:00:00.000','1996-07-15 00:00:00.000',4,41.34,'Victuailles en stock 2', " +
          "'rue du Commerce','Lyon',NULL,'69004','France','empFirstName2')");
      snc.sql("insert into orders values(10260,'CENTC',13,'1996-07-19 00:00:00.000'," +
          "'1996-08-16 00:00:00.000','1996-07-29 00:00:00.000',13,55.09,'Ottilies Kaoseladen'," +
          "'Mehrheimerstr. 369','Kaqln',NULL,'50739','Germany','empFirstName4')");
      snc.sql("insert into orders values(10265,'DUMON',18,'1996-07-25 00:00:00.000'," +
          "'1996-08-22 00:00:00.000','1996-08-12 00:00:00.000',18,55.28," +
          "'Blondel pare et fils 24', 'place Klacber','Strasbourg',NULL," +
          "'67000','France','empFirstName5')");
      var query1 = "SELECT * FROM orders where FirstName='empFirstName2'";
      var result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      query1 = "SELECT * FROM orders where FirstName='empFirstName4'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      query1 = "SELECT * FROM orders where FirstName='empFirstName5'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      snc.sql("delete from orders where FirstName='empFirstName2'");
      snc.sql("delete from orders where FirstName='empFirstName4'");
      snc.sql("delete from orders where FirstName='empFirstName5'");
      query1 = "SELECT * FROM orders where FirstName='empFirstName5'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      snc.alterTable("order_details", true, StructField("CustomerID", StringType, true))
      query = "SELECT * FROM order_details"
      result = snc.sql(query).schema.fields.length
      pw.println("Query : " + query + "\nResults : " + result)
      assert(result == 6)
      snc.sql("insert into order_details values(10250,72,34.8,5,0,'custID4')");
      snc.sql("insert into order_details values(10249,42,9.8,10,0,'custID14')");
      snc.sql("insert into order_details values(10253,41,7.7,10,0,'custID7')");
      query1 = "SELECT * FROM order_details where CustomerID='custID4'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      query1 = "SELECT * FROM order_details where CustomerID='custID14'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      query1 = "SELECT * FROM order_details where CustomerID='custID7'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      snc.sql("delete from order_details where CustomerID='custID4'");
      snc.sql("delete from order_details where CustomerID='custID14'");
      snc.sql("delete from order_details where CustomerID='custID7'");
      query1 = "SELECT * FROM order_details where CustomerID='custID14'";
      result1 = snc.sql(query1);
      pw.println("Query : " + query1 + "\nResult : " + result1.collect() + "\n " + result1.show())
      snc.alterTable("orders", false, StructField("FirstName", StringType, true))
      assert(snc.sql("SELECT * FROM " + "orders").schema.fields.length == 14)
      snc.alterTable("order_details", false, StructField("CustomerID", StringType, true))
      assert(snc.sql("SELECT * FROM order_details").schema.fields.length == 5)
      pw.println(s"Alter tables Test completed successfully at : " + System.currentTimeMillis)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/AlterTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
