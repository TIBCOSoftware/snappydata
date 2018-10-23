package io.snappydata.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import io.snappydata.SnappyFunSuite;
import io.snappydata.test.dunit.DistributedTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SnappyContext;
import org.junit.Test;

public class SnappyContextConcurrencySuite extends SnappyFunSuite {


  @Test
  @SuppressWarnings("unchecked")
  public void testMultithreadedAccess() {

    final AtomicLong counter = new AtomicLong();

    int poolSize = 5;
    List<SnappyQueryJob> tasks = new ArrayList<>();
    ExecutorService pool = Executors.newFixedThreadPool(poolSize);
    for (int i = 1; i <= poolSize; i++) {
      tasks.add(new SnappyQueryJob(SnappyContext.apply(sc()), i, counter));
    }

    long l1 = System.currentTimeMillis();
    List<Future<String>> futures ;
    try {
      futures = pool.invokeAll(tasks);

      DistributedTestBase.WaitCriterion ev = new DistributedTestBase.WaitCriterion() {
        public boolean done() {
         return counter.get() == 50;
        }
        public String description() {
          return null;
        }
      };
      DistributedTestBase.waitForCriterion(ev, 3*60*1000, 5000, true);
      pool.shutdownNow();

    } catch (InterruptedException e) {
      System.out.println("Thread interrupted");
    }
    long l2 = System.currentTimeMillis();
    System.out.println(" Time taken " + (l2 - l1));


  }
}

class SnappyQueryJob implements Callable<String>, Serializable {
  String threadId;
  transient org.apache.spark.sql.SnappyContext sqlContext;
  AtomicLong counter;


  public SnappyQueryJob(org.apache.spark.sql.SnappyContext _sqlContext, int
      id, AtomicLong ctr) {

    threadId = "thread_" + id;
    this.sqlContext = _sqlContext;
    this.counter = ctr;
  }

  public void actualWork() {
    for (int i = 0; i < 10; i++) {

      String tempTableName = threadId;
      String tblName = threadId + "_" + i;

      List<DummyBeanClass> dummyList = new ArrayList();
      for (int j = 0; j < 2; j++) {
        DummyBeanClass object = new DummyBeanClass();
        object.setCol2("" + i);
        object.setCol1(i);
        dummyList.add(object);
      }

      Dataset<Row> tempdf = sqlContext.emptyDataFrame();
      tempdf.registerTempTable(tempTableName);

      JavaSparkContext javaSparkContext = new JavaSparkContext(sqlContext.sparkContext());
      JavaRDD<DummyBeanClass> rdd = javaSparkContext.parallelize(dummyList);
      Dataset<Row> df = sqlContext.createDataFrame(rdd, DummyBeanClass.class);
      df.write().format("column").saveAsTable(tblName);
      String _query = String.format("select count(*) from %s", tblName);

      String _tempQuery = String.format("select count(*) from %s", tempTableName);


      List<Row> res;
      try {
        res = sqlContext.sql(_query).collectAsList();
        res = sqlContext.sql(_tempQuery).collectAsList();
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("*Exception " + debugTables() + "**");
        throw e;
      }
      sqlContext.dropTable(tblName, false);
      sqlContext.dropTable(tempTableName, false);
      //System.out.println(" dropped table " + tblName + " and tempTable " + tempTableName);
      counter.addAndGet(1);
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        System.out.println("Thread interrupted");
      }
    }
  }

  public String call() {
    try {
      actualWork();
    } catch (Throwable th) {
      th.printStackTrace();
    }

    return "success";
  }

  private String debugTables() {
    String v = Joiner.on(',').join(sqlContext.tableNames());
    if (v == null) return "";
    else return v;
  }

  public class DummyBeanClass implements Serializable {

    public Integer col1;
    public String col2;

    public Integer getCol1() {
      return col1;
    }

    public String getCol2() {
      return col2;
    }

    public void setCol1(Integer col1) {
      this.col1 = col1;
    }

    public void setCol2(String col2) {
      this.col2 = col2;
    }
  }
}
