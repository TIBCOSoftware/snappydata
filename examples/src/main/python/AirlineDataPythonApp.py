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

from pyspark.sql.types import *
from decimal import *
import os


# A Python example showing how create table in SnappyData and run queries
# To run this example use following command:
# bin/spark-submit <example>
# For example:
# bin/spark-submit quickstart/python/CreateTable.py

def createpartitionedtableusingsql(snappy):
    print("Creating partitioned table AIRLINE using SQL")
    snappy.sql("DROP TABLE IF EXISTS AIRLINE")
    # Create the table using SQL command
    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY),
    # For complete list of table attributes refer the documentation
    # http://snappydatainc.github.io/snappydata/rowAndColumnTables/
    snappy.sql("CREATE TABLE AIRLINE(" +
               "YearI             INTEGER NOT NULL," +
               "MonthI            INTEGER NOT NULL,"
               "DayOfMonth        INTEGER NOT NULL PRIMARY KEY," +
               "DayOfWeek         INTEGER NOT NULL," +
               "DepTime           INTEGER," +
               "CRSDepTime        INTEGER," +
               "ArrTime           INTEGER," +
               "CRSArrTime        INTEGER," +
               "UniqueCarrier     VARCHAR(20) NOT NULL,"
               "FlightNum         INTEGER," +
               "TailNum           VARCHAR(20)," +
               "ActualElapsedTime INTEGER," +
               "CRSElapsedTime    INTEGER," +
               "AirTime           INTEGER," +
               "ArrDelay          INTEGER," +
               "DepDelay          INTEGER," +
               "Origin            VARCHAR(20)," +
               "Dest              VARCHAR(20)," +
               "Distance          INTEGER," +
               "TaxiIn            INTEGER," +
               "TaxiOut           INTEGER," +
               "Cancelled         INTEGER," +
               "CancellationCode  VARCHAR(20)," +
               "Diverted          INTEGER," +
               "CarrierDelay      INTEGER," +
               "WeatherDelay      INTEGER," +
               "NASDelay          INTEGER," +
               "SecurityDelay     INTEGER," +
               "LateAircraftDelay INTEGER," +
               "ArrDelaySlot      INTEGER)" +
               "USING ROW OPTIONS (PARTITION_BY 'DayOfMonth')")
    print
    print("Inserting data in AIRLINE table using INSERT query")
    snappy.sql("INSERT INTO AIRLINE VALUES(2017, 06, 20, 02, 0715, 0715,1000, 1000,"
               " 'Delta', 0627, 'N123AA', 0245, 0245, 0245, 0, 0, 'PDX',"
               " 'LAX', 963, 0, 0, 0, 'ABC', 0, 15, 0, 0, 0, 0, 0)")
    print("Printing the contents of the AIRLINE table")
    snappy.sql("SELECT * FROM AIRLINE").show()

    snappy.sql("DROP TABLE IF EXISTS STAGING_AIRLINEREF")
    snappy.sql("DROP TABLE IF EXISTS AIRLINEREF")

    fullpath = os.path.abspath("quickstart/data/airportcodeParquetData")
    snappy.sql("CREATE EXTERNAL TABLE STAGING_AIRLINEREF"
               " USING parquet OPTIONS(path '{}')".format(fullpath))
    print ("Done creating external table")
    # ----- CREATE ROW TABLE -----

    snappy.sql("CREATE TABLE AIRLINEREF USING ROW OPTIONS() AS (SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF)")
    print ("Done creating airlineref")


# Constants
APP_NAME = " Python Airline Data Application "
COLUMN_TABLE_NAME = "airline"
ROW_TABLE_NAME = "airlineref"


def main(snappy):
    snappy.sql("set spark.sql.shuffle.partitions=6")
    # Get the tables that were created using sql scripts via snappy-sql
    airlinedf = snappy.table(COLUMN_TABLE_NAME)
    airlinecodedF = snappy.table(ROW_TABLE_NAME)

    # Data Frame query :Which Airlines Arrive On Schedule? JOIN with reference table
    colResult = airlinedf.alias('airlineDF') \
        .join(airlinecodedF.alias('airlineCodeDF'), col('airlineDF.UniqueCarrier') == col('airlineCodeDF.CODE')) \
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

    print("Airline arrival schedule after Updated values:")

    startColUpd = int(time.time() * 1000)
    totalTimeColUpd = int(time.time() * 1000) - startColUpd
    print("Query time:%dms" % totalTimeColUpd)


if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName('Python Example').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    snappy = SnappySession(sc)
    createpartitionedtableusingsql(snappy)
    main(snappy)
