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


import os
from datetime import date, datetime
import sys
import time
import tempfile
import struct
import shutil
from pyspark.streaming.tests import PySparkStreamingTestCase, BasicOperationTests, CheckpointTests, StreamingListenerTests, StreamingContextTests, WindowFunctionTests
from pyspark.streaming.snappy.context import SnappyStreamingContext , SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.snappy import SnappySession
from pyspark.sql.types import *
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

class SnappyBasicOperationTests(BasicOperationTests):

    def setUp(self):
        self.ssc = SnappyStreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(False)
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = SnappyStreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
        except:
            pass

class SnappyProgrammingGuideTests(unittest.TestCase):
    timeout = 10  # seconds
    duration = .5

    @classmethod
    def setUpClass(cls):
        class_name = cls.__name__
        conf = SparkConf().set("spark.default.parallelism", 1)
        cls.sc = SparkContext(appName=class_name, conf=conf)
        cls.sc.setCheckpointDir("/tmp")

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
            # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jSparkContextOption = SparkContext._jvm.SparkContext.get()
            if jSparkContextOption.nonEmpty():
               jSparkContextOption.get().stop()
        except:
             pass

    def setUp(self):
         self.ssc = SnappyStreamingContext(self.sc, self.duration)

    def tearDown(self):
         if self.ssc is not None:
            self.ssc.stop(False)
         # Clean up in the JVM just in case there has been some issues in Python API
         try:
            jStreamingContextOption = SnappyStreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
               jStreamingContextOption.get().stop(False)
         except:
            pass

    def test_schema_dstream(self):
        def rddList(start, end):
          return self.sc.parallelize(range(start, end)).map(lambda i: (i, "Text" + str(i)))

        def saveFunction(df):
           df.write.format("column").mode("append").saveAsTable("streamingExample")

        schema = StructType([StructField("loc", IntegerType()),
                             StructField("text", StringType())])

        snsc = SnappyStreamingContext(self.sc, 1)

        dstream = snsc.queueStream([rddList(1, 10), rddList(10, 20), rddList(20, 30)])

        snsc._snappySession.dropTable("streamingExample", True)
        snsc._snappySession.createTable("streamingExample", "column", schema)

        schemadstream = snsc.createSchemaDStream(dstream, schema)
        schemadstream.foreachDataFrame(lambda df: saveFunction(df))
        snsc.start()
        time.sleep(1)

        snsc.sql("select count(*) from streamingExample").show()


class SnappyStreamingListenerTests(StreamingListenerTests):
    def setUp(self):
        self.ssc = SnappyStreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
           self.ssc.stop(False)
    # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = SnappyStreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
               jStreamingContextOption.get().stop(False)
        except:
            pass

class SnappyWindowFunctionTests(WindowFunctionTests):
    def setUp(self):
        self.ssc = SnappyStreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
           self.ssc.stop(False)
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
             jStreamingContextOption = SnappyStreamingContext._jvm.SparkContext.getActive()
             if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
        except:
                pass

class SnappyStreamingContextTests(StreamingContextTests):

    def setUp(self):
        self.ssc = SnappyStreamingContext(self.sc, self.duration)

    def tearDown(self):
        if self.ssc is not None:
            self.ssc.stop(False)
        # Clean up in the JVM just in case there has been some issues in Python API
        try:
            jStreamingContextOption = SnappyStreamingContext._jvm.SparkContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop(False)
        except:
            pass

    def test_schema_dstream(self):
        rdd = [self.sc.parallelize([(127, -128, -32768, 32767, 2147483647, 1.0,
                                    date(2010, 1, 1), datetime(2010, 1, 1, 1, 1, 1),
                                    {"a": 1}, (2,), [1, 2, 3], None)])]
        schema = StructType([
          StructField("byte1", ByteType(), False),
          StructField("byte2", ByteType(), False),
          StructField("short1", ShortType(), False),
          StructField("short2", ShortType(), False),
          StructField("int1", IntegerType(), False),
          StructField("float1", FloatType(), False),
          StructField("date1", DateType(), False),
          StructField("time1", TimestampType(), False),
          StructField("map1", MapType(StringType(), IntegerType(), False), False),
          StructField("struct1", StructType([StructField("b", ShortType(), False)]), False),
          StructField("list1", ArrayType(ByteType(), False), False),
          StructField("null1", DoubleType(), True)])


        dstream = self.ssc.queueStream(rdd)
        self.ssc.sql("drop  table if exists testTable")

        self.ssc._snappySession.createTable("testTable", "column", schema)

        schemdstream = self.ssc.createSchemaDStream(dstream, schema)

        def testFunction(df):
            df.write.format("column").mode("append").saveAsTable("testTable")

        schemdstream.foreachDataFrame(lambda df: testFunction(df))

        self.ssc.sql("select count (*)  from testTable").collect()
        self.ssc.start()
        self.ssc.awaitTermination(2)
        result = SnappySession(self.sc).sql("select count(*) from testTable").collect()
        self.assertEqual(result[0][0], 1)

    def test_text_file_stream(self):
        d = tempfile.mkdtemp()
        self.ssc = SnappyStreamingContext(self.sc, self.duration)
        dstream2 = self.ssc.textFileStream(d).map(int)
        result = self._collect(dstream2, 2, block=False)
        self.ssc.start()
        for name in ('a', 'b'):
            time.sleep(1)
            with open(os.path.join(d, name), "w") as f:
                f.writelines(["%d\n" % i for i in range(10)])
        self.wait_for(result, 2)
        self.assertEqual([list(range(10)), list(range(10))], result)

    def test_binary_records_stream(self):
        d = tempfile.mkdtemp()
        self.ssc = SnappyStreamingContext(self.sc, self.duration)
        dstream = self.ssc.binaryRecordsStream(d, 10).map(
                lambda v: struct.unpack("10b", bytes(v)))
        result = self._collect(dstream, 2, block=False)
        self.ssc.start()
        for name in ('a', 'b'):
            time.sleep(1)
            with open(os.path.join(d, name), "wb") as f:
                f.write(bytearray(range(10)))
        self.wait_for(result, 2)
        self.assertEqual([list(range(10)), list(range(10))], [list(v[0]) for v in result])

    def test_get_active(self):
        self.assertEqual(SnappyStreamingContext.getActive(), None)

        # Verify that getActive() returns the active context
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(SnappyStreamingContext.getActive(), self.ssc)

        # Verify that getActive() returns None
        self.ssc.stop(False)
        self.assertEqual(SnappyStreamingContext.getActive(), None)

        # Verify that if the Java context is stopped, then getActive() returns None
        self.ssc = SnappyStreamingContext(self.sc, self.duration)
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(SnappyStreamingContext.getActive(), self.ssc)
        self.ssc._jssc.stop(False)
        self.assertEqual(SnappyStreamingContext.getActive(), None)

    def test_get_active_or_create(self):
        # Test StreamingContext.getActiveOrCreate() without checkpoint data
        # See CheckpointTests for tests with checkpoint data
        self.ssc = None
        self.assertEqual(SnappyStreamingContext.getActive(), None)

        def setupFunc():
            ssc = SnappyStreamingContext(self.sc, self.duration)
            ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
            self.setupCalled = True
            return ssc

        # Verify that getActiveOrCreate() (w/o checkpoint) calls setupFunc when no context is active
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

        # Verify that getActiveOrCreate() retuns active context and does not call the setupFunc
        self.ssc.start()
        self.setupCalled = False
        self.assertEqual(SnappyStreamingContext.getActiveOrCreate(None, setupFunc), self.ssc)
        self.assertFalse(self.setupCalled)

        # Verify that getActiveOrCreate() calls setupFunc after active context is stopped
        self.ssc.stop(False)
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

        # Verify that if the Java context is stopped, then getActive() returns None
        self.ssc = SnappyStreamingContext(self.sc, self.duration)
        self.ssc.queueStream([[1]]).foreachRDD(lambda rdd: rdd.count())
        self.ssc.start()
        self.assertEqual(SnappyStreamingContext.getActive(), self.ssc)
        self.ssc._jssc.stop(False)
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(None, setupFunc)
        self.assertTrue(self.setupCalled)

class SnappyCheckpointTests(CheckpointTests):
    @staticmethod
    def tearDownClass():
        # Clean up in the JVM just in case there has been some issues in Python API
        if SparkContext._jvm is not None:
            jStreamingContextOption = \
                SparkContext._jvm.org.apache.spark.streaming.SnappyStreamingContext.getActive()
            if jStreamingContextOption.nonEmpty():
                jStreamingContextOption.get().stop()


    def test_transform_function_serializer_failure(self):
        inputd = tempfile.mkdtemp()
        self.cpd = tempfile.mkdtemp("test_transform_function_serializer_failure")

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = SnappyStreamingContext(sc, 0.5)

            # A function that cannot be serialized
            def process(time, rdd):
                sc.parallelize(range(1, 10))

            ssc.textFileStream(inputd).foreachRDD(process)
            return ssc

        self.ssc = SnappyStreamingContext.getOrCreate(self.cpd, setup)
        try:
            self.ssc.start()
        except:
            import traceback
            failure = traceback.format_exc()
            self.assertTrue(
                    "It appears that you are attempting to reference SparkContext" in failure)
            return

        self.fail("using SparkContext in process should fail because it's not Serializable")

    def test_get_or_create_and_get_active_or_create(self):
        inputd = tempfile.mkdtemp()
        outputd = tempfile.mkdtemp() + "/"

        def updater(vs, s):
            return sum(vs, s or 0)

        def setup():
            conf = SparkConf().set("spark.default.parallelism", 1)
            sc = SparkContext(conf=conf)
            ssc = SnappyStreamingContext(sc, 0.5)
            dstream = ssc.textFileStream(inputd).map(lambda x: (x, 1))
            wc = dstream.updateStateByKey(updater)
            wc.map(lambda x: "%s,%d" % x).saveAsTextFiles(outputd + "test")
            wc.checkpoint(.5)
            self.setupCalled = True
            return ssc

        # Verify that getOrCreate() calls setup() in absence of checkpoint files
        self.cpd = tempfile.mkdtemp("test_streaming_cps")
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        self.ssc.start()

        def check_output(n):
            while not os.listdir(outputd):
                time.sleep(0.01)
            time.sleep(1)  # make sure mtime is larger than the previous one
            with open(os.path.join(inputd, str(n)), 'w') as f:
                f.writelines(["%d\n" % i for i in range(10)])

            while True:
                p = os.path.join(outputd, max(os.listdir(outputd)))
                if '_SUCCESS' not in os.listdir(p):
                    # not finished
                    time.sleep(0.01)
                    continue
                ordd = self.ssc.sparkContext.textFile(p).map(lambda line: line.split(","))
                d = ordd.values().map(int).collect()
                if not d:
                    time.sleep(0.01)
                    continue
                self.assertEqual(10, len(d))
                s = set(d)
                self.assertEqual(1, len(s))
                m = s.pop()
                if n > m:
                    continue
                self.assertEqual(n, m)
                break

        check_output(1)
        check_output(2)

        # Verify the getOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(3)

        # Verify that getOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify the getActiveOrCreate() recovers from checkpoint files
        self.ssc.stop(True, True)
        time.sleep(1)
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.ssc.start()
        check_output(4)

        # Verify that getActiveOrCreate() returns active context
        self.setupCalled = False
        self.assertEqual(SnappyStreamingContext.getActiveOrCreate(self.cpd, setup), self.ssc)
        self.assertFalse(self.setupCalled)

        # Verify that getActiveOrCreate() uses existing SparkContext
        self.ssc.stop(True, True)
        time.sleep(1)
        self.sc = SparkContext(conf=SparkConf())
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertFalse(self.setupCalled)
        self.assertTrue(self.ssc.sparkContext == self.sc)

        # Verify that getActiveOrCreate() calls setup() in absence of checkpoint files
        self.ssc.stop(True, True)
        shutil.rmtree(self.cpd)  # delete checkpoint directory
        time.sleep(1)
        self.setupCalled = False
        self.ssc = SnappyStreamingContext.getActiveOrCreate(self.cpd, setup)
        self.assertTrue(self.setupCalled)

        # Stop everything
        self.ssc.stop(True, True)



if __name__ == "__main__":
    testcases = [SnappyBasicOperationTests, SnappyWindowFunctionTests, SnappyStreamingContextTests,
             SnappyCheckpointTests, SnappyStreamingListenerTests, SnappyProgrammingGuideTests]
    sys.stderr.write("Running tests: %s \n" % (str(testcases)))
    failed = False
    for testcase in testcases:
        sys.stderr.write("[Running %s]\n" % (testcase))
        tests = unittest.TestLoader().loadTestsFromTestCase(testcase)
        if xmlrunner:
            result = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=3).run(tests)
            if not result.wasSuccessful():
                failed = True
        else:
            result = unittest.TextTestRunner(verbosity=3).run(tests)
            if not result.wasSuccessful():
                failed = True
    sys.exit(failed)
