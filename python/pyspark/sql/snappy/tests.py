# -*- encoding: utf-8 -*-
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
#

"""
Unit tests for pyspark.sql.snappy; additional tests are implemented as doctests in
individual modules.
"""
import os
import sys
import pydoc
import shutil
import tempfile
import pickle
import functools
import time
import datetime

import py4j

from pyspark.tests import ReusedPySparkTestCase
from pyspark.sql.tests import Row
from pyspark.sql.snappy import SnappyContext

try:
    import xmlrunner
except ImportError:
    xmlrunner = None

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

class SnappyContextTests(ReusedPySparkTestCase):
    testdata = [[1, 2, 3], [7, 8, 9], [1, 2, 3], [4, 2, 3], [5, 6, 7]]
    tablename = "TESTTABLE"

    def test_get_or_create(self):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        self.assertTrue(SnappyContext.getOrCreate(self.sc) is sqlcontext)

    def test_new_session(self):
        sqlcontext1 = SnappyContext.getOrCreate(self.sc)
        sqlcontext1.setConf("test_key", "a")

        sqlcontext2 = sqlcontext1.newSession()
        sqlcontext2.setConf("test_key", "b")

        self.assertEqual(sqlcontext1.getConf("test_key", ""), "a")
        self.assertEqual(sqlcontext2.getConf("test_key", ""), "b")

    def test_row_table_with_sql_api(self):
        ddl = "CREATE TABLE " + SnappyContextTests.tablename + \
              " (Col1 INT, Col2 INT, Col3 INT) " + " USING row OPTIONS (PARTITION_BY 'Col1')"
        self.create_table_using_sql(ddl, "row")
        self.verify_table_rows(5)

        self.truncate_table()
        self.verify_table_rows(0)

        self.drop_table()

    def test_column_table_with_sql_api(self):
        ddl = "Create Table " + SnappyContextTests.tablename + \
              " (Col1 INT, Col2 INT, Col3 INT) " + " USING column OPTIONS ()"
        self.create_table_using_sql(ddl, "column")
        self.verify_table_rows(5)

        self.truncate_table()
        self.verify_table_rows(0)

        self.drop_table()

    def test_row_table_with_datasource_api(self):
        self.drop_table(True)
        self.create_table_using_datasource("row")
        self.verify_table_rows(5)

        self.truncate_table()
        self.verify_table_rows(0)

        self.drop_table()

        self.create_table_using_datasource("row", True)
        self.verify_table_rows(5)

        self.drop_table()

    def test_column_table_with_datasource_api(self):
        self.drop_table(True)
        self.create_table_using_datasource("column")
        self.verify_table_rows(5)
        self.truncate_table()
        self.verify_table_rows(0)

    def test_update(self):
        self.drop_table(True)
        self.create_table_using_datasource("row")
        self.update_table()
        self.drop_table()

    def test_put(self):
         self.drop_table(True)
         self.create_table_using_datasource("row")
         self.put_table()
         self.drop_table()

    def test_insert(self):
        self.drop_table(True)
        self.create_table_using_datasource("row")
        self.insert_table()
        self.drop_table()


    def test_delete(self):
        self.drop_table(True)
        self.create_table_using_datasource("row")
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        self.assertTrue(sqlcontext.delete(SnappyContextTests.tablename, "col1=1"), 2)
        self.drop_table()

    def put_table(self):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        newrow = [1L, 2L, 3L], [2L, 3L, 4L]
        sqlcontext.put(SnappyContextTests.tablename, newrow)
        self.verify_table_rows(7)
        newrow = [1L, 2L, 3L]
        sqlcontext.put(SnappyContextTests.tablename , newrow)
        self.verify_table_rows(8)

    def insert_table(self):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        newrow = [1L, 2L, 3L], [2L, 3L, 4L]
        sqlcontext.insert(SnappyContextTests.tablename, newrow)
        self.verify_table_rows(7)
        newrow = [1L, 2L, 3L]
        sqlcontext.insert(SnappyContextTests.tablename , newrow)
        self.verify_table_rows(8)

    def update_table(self):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        newColumnvalues = Row(col1=7L)
        modifiedrows = sqlcontext.update(SnappyContextTests.tablename, "COL2 =2", newColumnvalues, "COL1")
        self.assertTrue(modifiedrows == 3)

    def truncate_table(self):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        sqlcontext.truncateTable(SnappyContextTests.tablename)

    def verify_table_rows(self, rowcount):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        result = sqlcontext.sql("SELECT COUNT(*) FROM " + SnappyContextTests.tablename).collect()
        self.assertTrue(result[0]._c0 == rowcount)

    def drop_table(self, ifexists=False):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        sqlcontext.dropTable(SnappyContextTests.tablename, ifexists)

    def create_table_using_sql(self, ddl, provider):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        dataDF = sqlcontext._sc.parallelize(SnappyContextTests.testdata, 5).toDF()
        sqlcontext.sql("DROP TABLE IF EXISTS " + SnappyContextTests.tablename)
        sqlcontext.sql(ddl)
        dataDF.write.format(provider).mode("append").saveAsTable(SnappyContextTests.tablename)

    def create_table_using_datasource(self, provider, schemaddl=False):
        sqlcontext = SnappyContext.getOrCreate(self.sc)
        df = sqlcontext._sc.parallelize(SnappyContextTests.testdata, 5).toDF(["COL1", "COL2", "COL3"])
        if schemaddl is False:
            sqlcontext.createTable(SnappyContextTests.tablename, provider, df.schema)
        else:
            sqlcontext.createTable(SnappyContextTests.tablename, provider, "(COL1 INT , COL2 INT , COL3 INT)")
        df.write.format("row").mode("append").saveAsTable(SnappyContextTests.tablename)


if __name__ == "__main__":
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
