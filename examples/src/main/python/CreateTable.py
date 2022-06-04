from pyspark.conf import SparkConf
from pyspark.sql.snappy import SnappySession
from pyspark.sql.types import StructField, StructType, IntegerType, DecimalType
from pyspark.context import SparkContext
from decimal import *

# A Python example showing how create table in SnappyData and run queries
# To run this example use following command:
# bin/spark-submit <example>
# For example:
# bin/spark-submit quickstart/python/CreateTable.py
def main(snappy):
    # creates table using SQL and runs queries on it
    createPartitionedTableUsingSQL(snappy)
    # creates table using API and runs queries on it
    createPartitionedTableUsingAPI(snappy)


def createPartitionedTableUsingSQL(snappy):
    print("Creating partitioned table PARTSUPP using SQL")
    snappy.sql("DROP TABLE IF EXISTS PARTSUPP")
    # Create the table using SQL command
    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY),
    snappy.sql("CREATE TABLE PARTSUPP ( " +
                  "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
                  "PS_SUPPKEY     INTEGER NOT NULL," +
                  "PS_AVAILQTY    INTEGER NOT NULL," +
                  "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
                  "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY' )")
    print
    print("Inserting data in PARTSUPP table using INSERT query")
    snappy.sql("INSERT INTO PARTSUPP VALUES(100, 1, 5000, 100)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(200, 2, 50, 10)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(300, 3, 1000, 20)")
    snappy.sql("INSERT INTO PARTSUPP VALUES(400, 4, 200, 30)")
    print("Printing the contents of the PARTSUPP table")
    snappy.sql("SELECT * FROM PARTSUPP").show()

    print
    print("Update the available quantity for PARTKEY 100")
    snappy.sql("UPDATE PARTSUPP SET PS_AVAILQTY = 50000 WHERE PS_PARTKEY = 100")
    print("Printing the contents of the PARTSUPP table after update")
    snappy.sql("SELECT * FROM PARTSUPP").show()

    print
    print("Delete the records for PARTKEY 400")
    snappy.sql("DELETE FROM PARTSUPP WHERE PS_PARTKEY = 400")
    print("Printing the contents of the PARTSUPP table after delete")
    snappy.sql("SELECT * FROM PARTSUPP").show()
    print("****Done****")

def createPartitionedTableUsingAPI(snappy):
    print
    print('Creating partitioned table PARTSUPP using API')

    # drop the table if it exists
    snappy.dropTable('PARTSUPP', True)

    schema = StructType([StructField('PS_PARTKEY', IntegerType(), False),
                     StructField('PS_SUPPKEY', IntegerType(), False),
                     StructField('PS_AVAILQTY', IntegerType(),False),
                     StructField('PS_SUPPLYCOST', DecimalType(15, 2), False)
                     ])

    # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY)
    snappy.createTable('PARTSUPP', 'row', schema, False, PARTITION_BY = 'PS_PARTKEY')

    print
    print("Inserting data in PARTSUPP table using dataframe")
    tuples = [(100, 1, 5000, Decimal(100)), (200, 2, 50, Decimal(10)),
              (300, 3, 1000, Decimal(20)), (400, 4, 200, Decimal(30))]
    rdd = sc.parallelize(tuples)
    tuplesDF = snappy.createDataFrame(rdd, schema)
    tuplesDF.write.insertInto("PARTSUPP")
    print("Printing the contents of the PARTSUPP table")
    snappy.sql("SELECT * FROM PARTSUPP").show()

    print("Update the available quantity for PARTKEY 100")
    snappy.update("PARTSUPP", "PS_PARTKEY =100", [50000], ["PS_AVAILQTY"])
    print("Printing the contents of the PARTSUPP table after update")
    snappy.sql("SELECT * FROM PARTSUPP").show()

    print("Delete the records for PARTKEY 400")
    snappy.delete("PARTSUPP", "PS_PARTKEY =400")
    print("Printing the contents of the PARTSUPP table after delete")
    snappy.sql("SELECT * FROM PARTSUPP").show()

    print("****Done****")


if __name__ == "__main__":
    sc = SparkContext('local[*]', 'Python Example')
    snappy = SnappySession(sc)
    main(snappy)



