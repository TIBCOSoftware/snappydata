from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.snappy import SnappyContext
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
import time
import sys
import exceptions
# This application depicts how a Spark cluster can
# connect to a Snappy cluster to fetch and query the tables
# using Python APIs in a Spark App.


## Constants
APP_NAME = " Python Airline Data Application "
COLUMN_TABLE_NAME = "airline"
ROW_TABLE_NAME = "airlineref"



if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("pyspark script logger initialized")
    print("created spark")
    snc = SnappyContext(sc)
    print("created snappy")
    snc.sql("set spark.sql.shuffle.partitions=6")
    # Get the tables that were created using sql scripts via snappy-shell
    LOGGER.info("Airline arrival schedule1")
    airlineDF = snc.table(COLUMN_TABLE_NAME)

    LOGGER.info("Airline arrival schedule2")
    airlineCodeDF = snc.table(ROW_TABLE_NAME)
    # main(snc, LOGGER)