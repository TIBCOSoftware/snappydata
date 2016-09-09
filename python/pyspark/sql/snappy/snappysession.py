#
# Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

from pyspark.sql.session import SparkSession

class SnappySession(SparkSession):

     def __init__(self, sparkContext, jsnappySession=None):
         """Creates a new SnappySession.
         """
         SparkSession.__init__(self, sparkContext)
         if jsnappySession is None:
            jsnappySession = self._jvm.SnappySession(self._jsc.sc())
         self._jsparkSession = jsnappySession

