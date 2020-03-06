/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.internal;

import io.snappydata.sql.catalog.SnappyExternalCatalog;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SnappySession;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEventListener;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener;

public final class SnappySharedState24 extends SnappySharedState {

  private volatile ExternalCatalogWithListener catalogWrapper;

  SnappySharedState24(SparkContext sparkContext) {
    super(sparkContext);

    if (this.embedCatalog != null) {
      // Wrap to provide catalog events
      this.catalogWrapper = new ExternalCatalogWithListener(this.embedCatalog);
      // Make sure we propagate external catalog events to the spark listener bus
      // noinspection Convert2Lambda
      this.catalogWrapper.addListener(new ExternalCatalogEventListener() {
        @Override
        public void onEvent(ExternalCatalogEvent event) {
          sparkContext().listenerBus().post(event);
        }
      });
    }
  }

  @Override
  public SnappyExternalCatalog getExternalCatalogInstance(SnappySession session) {
    if (this.embedCatalog != null) {
      return super.getExternalCatalogInstance(session);
    } else {
      synchronized (this) {
        SnappyExternalCatalog catalog = super.getExternalCatalogInstance(session);
        if (this.catalogWrapper == null) {
          this.catalogWrapper = new ExternalCatalogWithListener(catalog);
        }
        return catalog;
      }
    }
  }

  @Override
  public ExternalCatalogWithListener externalCatalog() {
    if (this.initialized) {
      return this.catalogWrapper;
    } else {
      // in super constructor, no harm in returning super's value at this point
      return super.externalCatalog();
    }
  }
}
