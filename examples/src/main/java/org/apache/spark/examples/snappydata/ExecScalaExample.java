package org.apache.spark.examples.snappydata;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * This example shows how one can use `exec scala` to execute a piece of scala/spark code
 * onto the ComputeDB cluster and get back the result.
 *
 * To start the cluster execute the following command:
 * <p/>
 *    ./sbin/snappy-start-all.sh
 * <p/>
 *
 * <p>
 *  Run the example using following command:
 * <pre>
 *     ./bin/run-example snappydata.ExecScalaExample
 * </pre>
 *
 */

public class ExecScalaExample {
  public static void main(String argArray[]) throws ClassNotFoundException, SQLException {

    // create JDBC connection
    Class.forName("io.snappydata.jdbc.ClientDriver");
    String url = "jdbc:snappydata://localhost:1527/";
    Connection connection = DriverManager.getConnection(url, new Properties());
    Statement statement = connection.createStatement();

    // execute some scala code via exec scala on ComputeDB cluster.
    String execScalaString = "exec scala " +
        "snappysession.sql(\"drop table if exists testtable\")\n " +
        "snappysession.sql(\"create table if not exists testtable (col1 int) using column\")\n" +
        "snappysession.sql(\"insert into testtable values (1),(2),(3)\")\n" +
        "val df1 = snappysession.sql(\"select * from testtable\").show";

    statement.execute(execScalaString);
    ResultSet resultSet1 = statement.getResultSet();

    System.out.println("ResultSet1");
    System.out.println(resultSet1.getMetaData().getColumnName(1));
    while (resultSet1.next()) {
      System.out.println(resultSet1.getString(1));
    }

    ResultSet resultSet2 = statement.executeQuery("exec scala options(returnDF 'ds2') case class " +
        "ClassData(col1: String, col2: Int)\n" +
        "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n" +
        "import sqlContext.implicits._\n" +
        "val ds1 = Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)).toDF(\"col1\", \"col2\")" +
        ".as[ClassData]\n" +
        "var rdd = sc.parallelize(Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)), 1)\n" +
        "val ds2 = rdd.toDF(\"col1\", \"col2\").as[ClassData]");

    System.out.println("\n\nResultSet2");
    System.out.println(resultSet2.getMetaData().getColumnName(1) + "  |  " +
        resultSet2.getMetaData().getColumnName(2));
    while (resultSet2.next()) {
      System.out.println(resultSet2.getString(1) + "     |  " + resultSet2.getInt(2));
    }
  }
}