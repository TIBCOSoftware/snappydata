/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.hive;

import io.snappydata.impl.SnappyHiveCatalog;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.ClusterMode;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.ThinClientConnectorMode;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog;
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils;
import org.apache.spark.sql.execution.ui.SQLListener;
import org.apache.spark.sql.execution.ui.SQLTab;
import org.apache.spark.sql.execution.ui.SnappySQLListener;
import org.apache.spark.sql.hive.client.HiveClient;
import org.apache.spark.sql.internal.SharedState;
import org.apache.spark.ui.SparkUI;

/**
 * Overrides Spark's SharedState to enable setting up own ExternalCatalog.
 */
public final class SnappySharedState extends SharedState {

  /**
   * A Hive client used to interact with the meta-store.
   */
  private volatile HiveClient client;

  /**
   * The ExternalCatalog implementation used for SnappyData (either
   * for embedded cluster or for connector).
   */
  private volatile SnappyExternalCatalog snappyCatalog;

  /**
   * Used to skip initializing meta-store in super's constructor.
   */
  private final boolean initialized;

  private static final String CATALOG_IMPLEMENTATION = "spark.sql.catalogImplementation";

  /**
   *  Create Snappy's SQL Listener instead of SQLListener
   */
  private static SQLListener createListenerAndUI(SparkContext sc) {
    SQLListener initListener = ExternalStoreUtils.getSQLListener().get();
    if (initListener == null) {
      SnappySQLListener listener = new SnappySQLListener(sc.conf());
      if (ExternalStoreUtils.getSQLListener().compareAndSet(null, listener)) {
        sc.addSparkListener(listener);
        scala.Option<SparkUI> ui = sc.ui();
        if (ui.isDefined()) {
          new SQLTab(listener, ui.get());
        }
      }
      return ExternalStoreUtils.getSQLListener().get();
    }
    else {
      return initListener;
    }
  }

  private SnappySharedState(SparkContext sparkContext) {
    super(sparkContext);
    this.initialized = true;
  }

  public static synchronized SnappySharedState create(SparkContext sparkContext) {
    // force in-memory catalog to avoid initializing hive for SnappyData
    final String catalogImpl = sparkContext.conf().get(CATALOG_IMPLEMENTATION, null);
    // there is a small thread-safety issue in that if multiple threads
    // are initializing normal concurrently SparkSession vs SnappySession
    // then former can land up with in-memory catalog too
    sparkContext.conf().set(CATALOG_IMPLEMENTATION, "in-memory");

    createListenerAndUI(sparkContext);

    final SnappySharedState sharedState = new SnappySharedState(sparkContext);

    // reset the catalog implementation to original
    if (catalogImpl != null) {
      sparkContext.conf().set(CATALOG_IMPLEMENTATION, catalogImpl);
    } else {
      sparkContext.conf().remove(CATALOG_IMPLEMENTATION);
    }
    return sharedState;
  }

  private synchronized void initMetaStore() {
    if (this.client != null) {
      assert this.snappyCatalog != null;
      return;
    }
    Boolean oldFlag = SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.get();
    SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.set(Boolean.TRUE);
    try {
      // avoid inheritance of activeSession
      SparkSession.clearActiveSession();
      this.client = HiveClientUtil$.MODULE$.newClient(sparkContext());
    } finally {
      SnappyHiveCatalog.SKIP_HIVE_TABLE_CALLS.set(oldFlag);
    }

    ClusterMode mode = SnappyContext.getClusterMode(sparkContext());
    if (mode instanceof ThinClientConnectorMode) {
      this.snappyCatalog = new SnappyConnectorExternalCatalog(metadataHive(),
          sparkContext().hadoopConfiguration());
    } else {
      this.snappyCatalog = new SnappyExternalCatalog(metadataHive(),
          sparkContext().hadoopConfiguration());
    }
  }

  public HiveClient metadataHive() {
    final HiveClient client = this.client;
    if (client != null) {
      return client;
    }
    initMetaStore();
    return this.client;
  }

  public SnappyExternalCatalog snappyCatalog() {
    final SnappyExternalCatalog snappyCatalog = this.snappyCatalog;
    if (snappyCatalog != null) {
      return snappyCatalog;
    }
    initMetaStore();
    return this.snappyCatalog;
  }

  @Override
  public ExternalCatalog externalCatalog() {
    if (this.initialized) {
      return snappyCatalog();
    } else {
      // in super constructor, no harm in returning super's value at this point
      return super.externalCatalog();
    }
  }
}
