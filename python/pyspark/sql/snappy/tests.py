#
# Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

"""
Unit tests for pyspark.sql.snappy; additional tests are implemented as doctests in
individual modules.
"""
import os
import sys

from pyspark.tests import ReusedPySparkTestCase
from pyspark.sql.types import *
from pyspark.sql.snappy import SnappySession

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
    testdata = ((1, 2, 3), (7, 8, 9), (1, 2, 3), (4, 2, 3), (5, 6, 7))
    tablename = "TESTTABLE"

    def test_new_session(self):
        sqlSession1 = SnappySession(self.sc)
        sqlSession1.conf.set("test_key", "a")

        sqlSession2 = sqlSession1.newSession()
        sqlSession2.conf.set("test_key", "b")

        self.assertEqual(sqlSession1.conf.get("test_key", ""), "a")
        self.assertEqual(sqlSession2.conf.get("test_key", ""), "b")

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
        sparkSession = SnappySession(self.sc)
        self.assertTrue(sparkSession.delete(SnappyContextTests.tablename, "col1=1"), 2)
        self.drop_table()

    def test_csv(self):
        self.drop_table(True)
        self.create_table_using_datasource("row")
        sparkSession = SnappySession(self.sc)
        csvPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../test_support/kv.txt")
        sparkSession.read.csv(csvPath).write.insertInto(tableName = SnappyContextTests.tablename)
        self.drop_table()

    def put_table(self):
        sparkSession = SnappySession(self.sc)
        newrow = ((1, 2, 3), (2, 3, 4))
        sparkSession.put(SnappyContextTests.tablename, newrow)
        self.verify_table_rows(7)
        newrow = [1, 2, 3]
        sparkSession.put(SnappyContextTests.tablename , newrow)
        self.verify_table_rows(8)

    def insert_table(self):
        sparkSession = SnappySession(self.sc)
        newrow = ((1, 2, 3), (2, 3, 4))
        sparkSession.insert(SnappyContextTests.tablename, newrow)
        self.verify_table_rows(7)
        newrow = [1, 2, 3]
        sparkSession.insert(SnappyContextTests.tablename , newrow)
        self.verify_table_rows(8)

    def update_table(self):
        sparkSession = SnappySession(self.sc)
        modifiedrows = sparkSession.update(SnappyContextTests.tablename, "COL2 =2", [7], ["COL1"])
        self.assertTrue(modifiedrows == 3)

    def truncate_table(self):
        sparkSession = SnappySession(self.sc)
        sparkSession.truncateTable(SnappyContextTests.tablename, True)

    def verify_table_rows(self, rowcount):
        sparkSession = SnappySession(self.sc)
        result = sparkSession.sql("SELECT COUNT(*) FROM " + SnappyContextTests.tablename).collect()
        self.assertTrue(result[0][0] == rowcount)

    def drop_table(self, ifexists=False):
        sparkSession = SnappySession(self.sc)
        sparkSession.dropTable(SnappyContextTests.tablename, ifexists)

    def create_table_using_sql(self, ddl, provider):
        sparkSession = SnappySession(self.sc)
        schema = StructType().add("col1", IntegerType()).add("col2", IntegerType()).add("col3", IntegerType())
        input = SnappyContextTests.testdata
        dataDF = sparkSession.createDataFrame(input, schema)
        sparkSession.sql("DROP TABLE IF EXISTS " + SnappyContextTests.tablename)
        sparkSession.sql(ddl)
        dataDF.write.insertInto(SnappyContextTests.tablename)

    def create_table_using_datasource(self, provider, schemaddl=False):
        sparkSession = SnappySession(self.sc)
        schema = StructType().add("col1", IntegerType()).add("col2", IntegerType()).add("col3", IntegerType())
        input = SnappyContextTests.testdata
        df = sparkSession.createDataFrame(input, schema)
        if schemaddl is False:
            sparkSession.createTable(SnappyContextTests.tablename, provider, schema)
        else:
            sparkSession.createTable(SnappyContextTests.tablename, provider, "(COL1 INT , COL2 INT , COL3 INT)")
        df.write.format("row").mode("append").saveAsTable(SnappyContextTests.tablename)


if __name__ == "__main__":
    if xmlrunner:
        unittest.main(testRunner=xmlrunner.XMLTestRunner(output='target/test-reports'))
    else:
        unittest.main()
