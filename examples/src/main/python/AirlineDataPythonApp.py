from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.snappy import SnappySession
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
import time
import sys

# This application depicts how a Spark cluster can
# connect to a Snappy cluster to fetch and query the tables
# using Python APIs in a Spark App.


## Constants
APP_NAME = " Python Airline Data Application "
COLUMN_TABLE_NAME = "airline"
ROW_TABLE_NAME = "airlineref"


def main(snappy):
    snappy.sql("set spark.sql.shuffle.partitions=6")
    # Get the tables that were created using sql scripts via snappy-shell
    airlineDF = snappy.table(COLUMN_TABLE_NAME)
    airlineCodeDF = snappy.table(ROW_TABLE_NAME)

    # Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    colResult = airlineDF.alias('airlineDF') \
        .join(airlineCodeDF.alias('airlineCodeDF'), col('airlineDF.UniqueCarrier') == col('airlineCodeDF.CODE')) \
        .groupBy(col('airlineDF.UniqueCarrier'), col('airlineCodeDF.Description')) \
        .agg({"ArrDelay": "avg"}). \
        orderBy("avg(ArrDelay)")

    print("Airline arrival schedule")
    start = int(time.time() * 1000)
    colResult.show()
    totalTimeCol = int(time.time() * 1000) - start
    print("Query time: %dms" % totalTimeCol)

    # Suppose a particular Airline company say 'Delta Air Lines Inc.'
    # re-brands itself as 'Delta America'.Update the row table.
    query = " CODE ='DL'"
    newColumnValues = ["Delta America Renewed"]
    snappy.update(ROW_TABLE_NAME, query, newColumnValues, ["DESCRIPTION"])

    # Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    colResultAftUpd = airlineDF.alias('airlineDF') \
        .join(airlineCodeDF.alias('airlineCodeDF'), col('airlineDF.UniqueCarrier') == col('airlineCodeDF.CODE')) \
        .groupBy(col('airlineDF.UniqueCarrier'), col('airlineCodeDF.Description')) \
        .agg({"ArrDelay": "avg"}). \
        orderBy("avg(ArrDelay)")

    print("Airline arrival schedule after Updated values:")

    startColUpd = int(time.time() * 1000)
    colResultAftUpd.show()
    totalTimeColUpd = int(time.time() * 1000) - startColUpd
    print("Query time:%dms" % totalTimeColUpd)

if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    snc = SnappySession(sc)
    main(snc)
