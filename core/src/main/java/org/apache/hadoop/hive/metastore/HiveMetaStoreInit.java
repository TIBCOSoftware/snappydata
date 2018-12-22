package org.apache.hadoop.hive.metastore;

public class HiveMetaStoreInit {
  public static void initNullType() {
    MetaStoreUtils.hiveThriftTypeMap.add("null");
  }
}
