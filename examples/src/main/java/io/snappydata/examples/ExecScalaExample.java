package io.snappydata.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

public class ExecScalaExample {
  public static void main(String argArray[]) throws IOException, ClassNotFoundException,
      SQLException {
    // some java code
    File file = new File("/tmp/exec_scala_example_output.txt");
    FileWriter fileWriter = new FileWriter(file);

    // create JDBC connection
    Class.forName("io.snappydata.jdbc.ClientDriver");
    String url = "jdbc:snappydata://localhost:1527/";
    Connection connection = DriverManager.getConnection(url, new Properties());
    Statement statement = connection.createStatement();

    // fire some scala code via exec scala
    String execScalaString = "exec scala options(returnDF 'dff') " +
        "snappysession.sql(\"drop table if exists testtable\")\n " +
        "snappysession.sql(\"create table if not exists testtable (col1 int) using column\")\n" +
        "snappysession.sql(\"insert into testtable values (1),(2),(3)\")\n" +
        "val dff = snappysession.sql(\"select * from testtable\")";

    statement.execute(execScalaString);
    ResultSet resultSet1 = statement.getResultSet();

    System.out.println(resultSet1.getMetaData().getColumnName(1));
    while (resultSet1.next()) {
      System.out.println(resultSet1.getString(1));
    }

    ResultSet resultSet2 = statement.executeQuery("exec scala options(returnDF 'ds2') case class " +
        "ClassData(a: String, b: Int)\n" +
        "val sqlContext = new org.apache.spark.sql.SQLContext(sc)\n" +
        "import sqlContext.implicits._\n" +
        "val ds1 = Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)).toDF(\"a\", \"b\").as[ClassData]\n" +
        "var rdd = sc.parallelize(Seq((\"a\", 1), (\"b\", 2), (\"c\", 3)), 1)\n" +
        "val ds2 = rdd.toDF(\"a\", \"b\").as[ClassData]");

    while (resultSet2.next()) {
      fileWriter.append(resultSet2.getString(1) + "  |  " + resultSet2.getInt(2) + "\n");
    }
    fileWriter.close();
  }
}