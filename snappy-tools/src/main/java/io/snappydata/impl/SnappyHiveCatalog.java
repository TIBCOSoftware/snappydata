package io.snappydata.impl;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.pivotal.gemfirexd.internal.catalog.ExternalCatalog;
import com.pivotal.gemfirexd.internal.engine.Misc;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.ExternalTableType;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by kneeraj on 10/1/15.
 */
public class SnappyHiveCatalog implements ExternalCatalog {

  final private HiveMetaStoreClient hmc;

  final InternalDistributedMember thisMember;

  public SnappyHiveCatalog() {
    // initialize HiveMetaStoreClient
    String snappydataurl = "jdbc:snappydata:;route-query=false;user=HIVE_METASTORE";

    HiveConf metadataConf = new HiveConf();
    metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        snappydataurl);
    metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
      "com.pivotal.gemfirexd.jdbc.EmbeddedDriver");

    try {
      this.hmc = new HiveMetaStoreClient(metadataConf);
    } catch (MetaException me) {
      throw new IllegalStateException(me);
    }
    thisMember = InternalDistributedSystem.getConnectedInstance().getDistributedMember();
  }

  public boolean isColumnTable(String tableName) {
    try {
      String tableType = getType(tableName);
      if (tableType != null && tableType.equals(ExternalTableType.Columnar().toString())) {
        return true;
      }
    } catch (TException e) {
      throw new IllegalArgumentException(e);
    }
    return false;
  }

  public boolean isRowTable(String tableName) {
    try {
      String tableType = getType(tableName);
      //System.out.println("KN: tableType = " + tableType);
      if (tableType != null && tableType.equals(ExternalTableType.Row().toString())) {
        return true;
      }
    } catch (TException e) {
      throw new IllegalArgumentException(e);
    }
    return false;
  }

  public boolean tableExists(String tableName) {
    try {
      this.hmc.getTable("", tableName);
    } catch (TException e) {
      return false;
    }
    return true;
  }

  // TODO: Will be implemented later when the serDe actually carries
  // this information
  public boolean hasComplexTypes(String tableName) {
    return false;
  }

  // TODO: Will be implemented later when the serDe actually carries
  // this information
  public boolean hasUserDefinedTypes(String tableName) {
    return false;
  }

  // TODO: Will be implemented later when the serDe actually carries
  // this information
  public boolean isSnappyUDF(String fnName) {
    return false;
  }

  public String getCatalogDescription() {
    return "Snappy Hive Catalog Client [" + thisMember + "]";
  }

  private String getType(String tableName) throws TException {
    List<String> list = this.hmc.getAllDatabases();
//    Misc.getCacheLogWriter().info("KN: db list = " + list, new Exception());
//    for (String s : list) {
//      Misc.getCacheLogWriter().info("KN: db = " + s);
//    }
    List<String> tables = this.hmc.getAllTables("default");
//    for (String s : tables) {
//      //System.out.println("table in default = " + s);
//      Misc.getCacheLogWriter().info("KN: table = " + s);
//    }
    Table t = this.hmc.getTable("default", tableName);
    String type = t.getTableType();
//    System.out.println("KN: table type = " + type + " for ");
//    Misc.getCacheLogWriter().info("KN: table type = " + type + " for ");
    Map<String, String> props = t.getParameters();//.getSd().getSerdeInfo().getParameters();
    Set<String> s = props.keySet();
//    for(String p : s) {
//      System.out.println("KN: Key = " + p + " val = " + props.get(p));
//      Misc.getCacheLogWriter().info("KN: Key = " + p + " val = " + props.get(p));
//    }
    return t.getParameters().get("EXTERNAL");
  }
}