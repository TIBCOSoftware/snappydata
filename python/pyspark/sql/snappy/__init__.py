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


"""
Important classes of Snappy SQL and DataFrames:

    - :class:`pyspark.sql.SQLContext`
      Main entry point for :class:`DataFrame` and SQL functionality.
    - :class:`pyspark.sql.snappy.SnappyContext`
      Main entry point for accessing data stored in SnappyData.
"""

from __future__ import absolute_import

from pyspark.sql.snappy.context import SnappyContext
from pyspark.sql.snappy.snappysession import SnappySession

__all__ = ['SnappySession', 'SnappyContext']
