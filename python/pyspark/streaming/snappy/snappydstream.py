#
# Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
#
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.


from pyspark.streaming.dstream import DStream
from pyspark.sql.snappy import SnappySession

class SchemaDStream(DStream):
    """
     A SQL based DStream with support for schema/Product
     This class offers the ability to manipulate SQL query on DStreams
     It is similar to SchemaRDD, which offers the similar functions
     Internally, RDD of each batch duration is treated as a small
     table and CQs are evaluated on those small tables
     Some of the abstraction and code is borrowed from the project:
     https://github.com/Intel-bigdata/spark-streamingsql
     @param snsc
     @param queryExecution
    """

    def __init__(self, jdstream, ssc, jrdd_deserializer, schema):
        DStream.__init__(self, jdstream, ssc, jrdd_deserializer)

        self._schema = schema
        self._snappySession = SnappySession(self._sc)

    def foreachDataFrame(self, func):

        def createDataFrame(_, rdd):
            df = self._snappySession.createDataFrame(rdd, self._schema)
            func(df)
        self.foreachRDD(createDataFrame)
