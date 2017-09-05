from __future__ import print_function
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.snappy import SnappySession
from pyspark.rdd import *
from pyspark.ml.clustering import KMeans, KMeansModel
import random
import os


def createPartitionedTableUsingSQL(snappy):
    snappy.sql("DROP TABLE IF EXISTS WEATHER")
    snappy.sql("CREATE TABLE WEATHER(" +
               "id 		integer NOT NULL PRIMARY KEY," +
               "DayOfMonth		FLOAT NOT NULL ," +
               "WeatherDegrees		FLOAT NOT NULL)" +
               "USING ROW OPTIONS (PARTITION_BY 'DayOfMonth')")

    print
    print("Inserting data into WEATHER table")
    counter = 0
    while counter < 100:
        counter = counter + 1
        snappy.sql("INSERT INTO WEATHER VALUES (" + str(counter) + "," + str(random.randint(1, 32)) + "," + str(
            random.randint(1, 120)) + ")")
    print("printing contents of WEATHER table")
    snappy.sql("SELECT * FROM WEATHER").show(100)

    print("DONE")


def applyKMeans(snappy):
    # Selects and parses the data from the table created earlier
    data = snappy.sql("SELECT id, WeatherDegrees FROM WEATHER")
    parsedData = data.rdd.map(lambda row: (row["ID"], str(row["WEATHERDEGREES"])))
    result = sorted(parsedData.collect(), key=lambda tup: tup[0])

    # Writes the data into the parsedData text file for training
    print("Writing parsed data to weatherdata/parsedData.txt")
    if not os.path.exists("weatherdata"):
        os.makedirs("weatherdata")
    a = open("weatherdata/parsedData.txt", 'w')
    c = 0
    for y in result:
        x = str(c) + " " + "1:" + str(y[1]) + " " + "2:" + str(y[1]) + " " + "3:" + str(y[1])
        print(x)
        a.write(x + "\n")
        c = c + 1

    a.close()

    # Trains the data in order to pass it to the Kmeans Clustering Function
    dataset = snappy.read.format("libsvm").load("weatherdata/parsedData.txt")
    print("dataset is " + str(dataset))
    kmeans = KMeans().setK(4).setSeed(2)
    model = kmeans.fit(dataset)

    # Evaluate clustering by computing Within Set Sum of Squared Errors.
    wssse = model.computeCost(dataset)
    print("Within Set Sum of Squared Errors = " + str(wssse))

    # Shows the result, as both the cluster centers, and a table with the cluster assignments in the Predictions column
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    transformedDF = model.transform(dataset)
    transformedDF.show(100)


def main(snappy):
    createPartitionedTableUsingSQL(snappy)
    applyKMeans(snappy)
    print("FINISHED ##########")


if __name__ == "__main__":
    # Configure Spark

    conf = SparkConf().setAppName('SnappyData KMeans').setMaster("local[*]")
    sc = SparkContext(conf=conf)
    snappy = SnappySession(sc)
    main(snappy)
