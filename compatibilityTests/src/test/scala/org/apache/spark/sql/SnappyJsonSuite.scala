package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.json.JsonSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

class SnappyJsonSuite extends JsonSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

}
