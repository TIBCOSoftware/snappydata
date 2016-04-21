#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from py4j.protocol import Py4JError
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame

__all__ = ["SnappyContext"]


class SnappyContext(SQLContext):
    """A variant of Spark SQL that integrates with data stored in SnappyData.
    :param sparkContext: The SparkContext to wrap.
    :param snappyContext: An optional JVM Scala SnappyContext. If set, we do not instantiate a new
        :class:`SnappyContext` in the JVM, instead we make all calls to this object.
    """

    def __init__(self, sparkContext, snappyContext=None):
        SQLContext.__init__(self, sparkContext)
        if snappyContext:
            self._scala_SnappyContext = snappyContext

    @classmethod
    def getOrCreate(cls, sc):
        """
        Get the existing SnappyContext or create a new one with given SparkContext.
        :param sc: SparkContext
        """
        if cls._instantiatedContext is None:
            jsqlContext = sc._jvm.SnappyContext.getOrCreate(sc._jsc.sc())
            cls(sc, jsqlContext)
        return cls._instantiatedContext

    @property
    def _ssql_ctx(self):
        try:
            if not hasattr(self, '_scala_SnappyContext'):
                self._scala_SnappyContext = self._get_snappy_ctx()
            return self._scala_SnappyContext
        except Py4JError as e:
            raise Exception("You must build Spark with SnappyData. to build run "
                            "./gradlew product ", e)

    def _get_snappy_ctx(self):
        return self._jvm.SnappyContext(self._jsc.sc())

    def createTable(self, tableName, provider=None, schema=None,  **options):
        """
        Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
        supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
        API creates a persistent catalog entry.
        :param tableName Name of the table
        :param provider  Provider name 'ROW' and 'JDBC'.
        :param schema Table schema either as a StructType or  String
                schemaStringExample = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
        :param options   Properties for table creation. See options list for different tables.
        :return: :class:`DataFrame`
         """
        if provider is None:
            provider = self.getConf("spark.sql.sources.default", "org.apache.spark.sql.parquet")
        if schema is None:
            df = self._ssql_ctx.createTable(tableName, provider, options)
        else:
            if isinstance(schema, str):
                df = self._ssql_ctx.createTable(tableName, provider, schema, options)
            elif not isinstance(schema, StructType):
                raise TypeError("schema should be StructType or a String")
            else:
                scala_datatype = self._ssql_ctx.parseDataType(schema.json())
                df = self._ssql_ctx.createTable(tableName, provider, scala_datatype, options)

        return DataFrame(df, self)

    def truncateTable(self, tableName):
        """
        Empties the contents of the table without deleting the catalog entry.
        :param tableName table to be dropped
        """
        self._ssql_ctx.truncateTable(tableName)

    def dropTable(self, tableName, ifExists=False):
        """
        Drop a SnappyData table created by a call to SnappyContext.createTable
        :param tableName table to be dropped
        :param ifExists  attempt drop only if the table exists
        """
        return self._ssql_ctx.dropTable(tableName, ifExists)

    def insert(self, tableName, rows):
        """
        Insert one or more [[org.apache.spark.sql.Row]] into an existing table
         A user can insert a DataFrame using foreachPartition.
        :param tableName:
        :param rows:
        :return: Number of rows inserted
        """
        if isinstance(rows, list):
            return self._ssql_ctx.insert(tableName, [rows, ])
        elif isinstance(rows, tuple):
            return self._ssql_ctx.insert(tableName, rows)
        else:
            raise TypeError("rows should be tuple or a list")

    def put(self, tableName, rows):
        """
        Upsert one or more [[org.apache.spark.sql.Row]] into an existing table
        upsert a DataFrame using foreachPartition...
        :param tableName:
        :param row:
        :return:  Number of rows inserted
        """
        if isinstance(rows, list):
            return self._ssql_ctx.put(tableName, [rows, ])
        elif isinstance(rows, tuple):
            return self._ssql_ctx.put(tableName, rows)
        else:
            raise TypeError("rows should be tuple or a list")

    def update(self, tableName, filterExpr, newColumnValues, *updateColumns):
        """
        update all rows in table that match passed filter expression
        :param tableName: Table name which needs to be updated
        :param filterExpr: SQL WHERE criteria to select rows that will be updated
        :param newColumnValues:  single Row containing all updated column values.
        They MUST match the updateColumn list
        :param updateColumns   List of all column names being updated
        :return: List of all column names being updated
        """
        if isinstance(newColumnValues, tuple):
            return self._ssql_ctx.update(tableName, filterExpr, newColumnValues, updateColumns)
        else:
            raise TypeError("newColumnValues should be tuple")

    def delete(self, tableName, filterExpr):
        """
        Delete all rows in table that match passed filter expression
        :param tableName:
        :param filterExpr: SQL WHERE criteria to select rows that will be deleted
        :return: number of rows deleted
        """
        return self._ssql_ctx.delete(tableName, filterExpr)



