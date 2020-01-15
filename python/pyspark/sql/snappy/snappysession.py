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


from pyspark.sql.types import StructType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession, _monkey_patch_RDD

__all__ = ["SnappySession"]

class SnappySession(SparkSession):
    def __init__(self, sparkContext, jsparkSession=None):
        """Creates a new SnappySession.
        """
        self._sc = sparkContext
        self._jsc = self._sc._jsc
        self._jvm = self._sc._jvm
        SparkSession.__init__(self, sparkContext)
        if jsparkSession is None:
            jsparkSession = self._jvm.SnappySession(self._jsc.sc())

        from pyspark.sql.snappy import SnappyContext
        self._wrapped = SnappyContext(self._sc, jsparkSession)
        self._jsparkSession = jsparkSession


    def newSession(self):
        """
       Returns a new SparkSession as new session, that has separate SQLConf,
       registered temporary views and UDFs, but shared SparkContext and
       table cache.
       """
        return self.__class__(self._sc, self._jsparkSession.newSession())

    def createTable(self, tableName, provider=None, schema=None, allowExisting=True, **options):
        """
        Creates a Snappy managed table. Any relation providers (e.g. parquet, jdbc etc)
        supported by Spark & Snappy can be created here. Unlike SqlContext.createExternalTable this
        API creates a persistent catalog entry.
        :param tableName Name of the table
        :param provider  Provider name 'ROW' and 'JDBC'.
        :param schema Table schema either as a StructType or  String
                schemaStringExample = "(OrderId INT NOT NULL PRIMARY KEY,ItemId INT, ITEMREF INT)"
        :param allowExisting When set to true it will ignore if a table with the same name is
                          present , else it will throw table exist exception
        :param options   Properties for table creation. See options list for different tables.
        :return: :class:`DataFrame`
        """
        if provider is None:
            provider = self.conf.get("spark.sql.sources.default", "org.apache.spark.sql.parquet")
        if schema is None:
            df = self._jsparkSession.createTable(tableName, provider, options, allowExisting)
        else:
            if isinstance(schema, str):
                df = self._jsparkSession.createTable(tableName, provider, schema, options, allowExisting)
            elif not isinstance(schema, StructType):
                raise TypeError("schema should be StructType or a String")
            else:
                scala_datatype = self._jsparkSession.parseDataType(schema.json())
                df = self._jsparkSession.createTable(tableName, provider, scala_datatype, options, allowExisting)

        return DataFrame(df, self)

    def truncateTable(self, tableName, ifExists=False):
        """
        Empties the contents of the table without deleting the catalog entry.
        :param tableName table to be dropped
        """
        self._jsparkSession.truncateTable(tableName, ifExists)

    def dropTable(self, tableName, ifExists=False):
        """
        Drop a SnappyData table created by a call to SnappyContext.createTable
        :param tableName table to be dropped
        :param ifExists  attempt drop only if the table exists
        """
        return self._jsparkSession.dropTable(tableName, ifExists)

    def insert(self, tableName, rows):
        """
        Insert one or more [[org.apache.spark.sql.Row]] into an existing table
         A user can insert a DataFrame using foreachPartition.
        :param tableName:
        :param rows:
        :return: Number of rows inserted
        """
        if isinstance(rows, list):
            return self._jsparkSession.insert(tableName, [rows, ])
        elif isinstance(rows, tuple):
            return self._jsparkSession.insert(tableName, rows)
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
            return self._jsparkSession.put(tableName, [rows, ])
        elif isinstance(rows, tuple):
            return self._jsparkSession.put(tableName, rows)
        else:
            raise TypeError("rows should be tuple or a list")

    def update(self, tableName, filterExpr, newColumnValues, updateColumns):
        """
        update all rows in table that match passed filter expression
        :param tableName: Table name which needs to be updated
        :param filterExpr: SQL WHERE criteria to select rows that will be updated
        :param newColumnValues:  single list containing all updated column values.
        They MUST match the updateColumn list
        :param updateColumns   List of all column names being updated
        :return: List of all column names being updated
        """
        if isinstance(newColumnValues, list) and isinstance(updateColumns, list):
            return self._jsparkSession.update(tableName, filterExpr, newColumnValues, updateColumns)
        else:
            raise TypeError("newColumnValues and updateColumns should be list")

    def delete(self, tableName, filterExpr):
        """
        Delete all rows in table that match passed filter expression
        :param tableName:
        :param filterExpr: SQL WHERE criteria to select rows that will be deleted
        :return: number of rows deleted
        """
        return self._jsparkSession.delete(tableName, filterExpr)