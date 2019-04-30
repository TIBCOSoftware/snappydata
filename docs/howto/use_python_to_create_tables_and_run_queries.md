<a id="howto-python"></a>
# How to use Python to Create Tables and Run Queries

Developers can write programs in Python to use TIBCO ComputeDB features. 

**First create a Snappy Session**:

```pre
 from pyspark.sql.snappy import Snappy Session
 from pyspark import SparkContext, SparkConf

 conf = SparkConf().setAppName(appName).setMaster(master)
 sc = SparkContext(conf=conf)
 snappy = SnappySession(sc)
```

**Create table using Snappy Session**:

```pre
# Creating partitioned table PARTSUPP using SQL
snappy.sql("DROP TABLE IF EXISTS PARTSUPP")
# "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY),
# For complete list of table attributes refer the documentation
# http://snappydatainc.github.io/snappydata/programming_guide
snappy.sql("CREATE TABLE PARTSUPP ( " +
      "PS_PARTKEY     INTEGER NOT NULL PRIMARY KEY," +
      "PS_SUPPKEY     INTEGER NOT NULL," +
      "PS_AVAILQTY    INTEGER NOT NULL," +
      "PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL)" +
      "USING ROW OPTIONS (PARTITION_BY 'PS_PARTKEY' )")
```

**Inserting data in table using INSERT query**:

```pre
snappy.sql("INSERT INTO PARTSUPP VALUES(100, 1, 5000, 100)")
snappy.sql("INSERT INTO PARTSUPP VALUES(200, 2, 50, 10)")
snappy.sql("INSERT INTO PARTSUPP VALUES(300, 3, 1000, 20)")
snappy.sql("INSERT INTO PARTSUPP VALUES(400, 4, 200, 30)")
# Printing the contents of the PARTSUPP table
snappy.sql("SELECT * FROM PARTSUPP").show()
```

**Update the data using SQL**:

```pre
# Update the available quantity for PARTKEY 100
snappy.sql("UPDATE PARTSUPP SET PS_AVAILQTY = 50000 WHERE PS_PARTKEY = 100")
# Printing the contents of the PARTSUPP table after update
snappy.sql("SELECT * FROM PARTSUPP").show()
```    

**Delete records from the table**:
```pre
# Delete the records for PARTKEY 400
snappy.sql("DELETE FROM PARTSUPP WHERE PS_PARTKEY = 400")
# Printing the contents of the PARTSUPP table after delete
snappy.sql("SELECT * FROM PARTSUPP").show()
```

**Create table using API**:
This same table can be created by using createTable API. First create a schema and then create the table, and then mutate the table data using API:

```pre
# drop the table if it exists
snappy.dropTable('PARTSUPP', True)

schema = StructType([StructField('PS_PARTKEY', IntegerType(), False),
      StructField('PS_SUPPKEY', IntegerType(), False),
      StructField('PS_AVAILQTY', IntegerType(),False),
      StructField('PS_SUPPLYCOST', DecimalType(15, 2), False)
      ])

 # "PARTITION_BY" attribute specifies partitioning key for PARTSUPP table(PS_PARTKEY)
 # For complete list of table attributes refer the documentation at
 # http://snappydatainc.github.io/snappydata/programming_guide
 snappy.createTable('PARTSUPP', 'row', schema, False, PARTITION_BY = 'PS_PARTKEY')

 # Inserting data in PARTSUPP table using DataFrame
tuples = [(100, 1, 5000, Decimal(100)), (200, 2, 50, Decimal(10)),
         (300, 3, 1000, Decimal(20)), (400, 4, 200, Decimal(30))]
rdd = sc.parallelize(tuples)
tuplesDF = snappy.createDataFrame(rdd, schema)
tuplesDF.write.insertInto("PARTSUPP")
#Printing the contents of the PARTSUPP table
snappy.sql("SELECT * FROM PARTSUPP").show()

# Update the available quantity for PARTKEY 100
snappy.update("PARTSUPP", "PS_PARTKEY =100", [50000], ["PS_AVAILQTY"])
# Printing the contents of the PARTSUPP table after update
snappy.sql("SELECT * FROM PARTSUPP").show()

# Delete the records for PARTKEY 400
snappy.delete("PARTSUPP", "PS_PARTKEY =400")
# Printing the contents of the PARTSUPP table after delete
snappy.sql("SELECT * FROM PARTSUPP").show()
```

The complete source code for the above example is in [CreateTable.py](https://github.com/SnappyDataInc/snappydata/blob/master/examples/src/main/python/CreateTable.py)

**Related Topics:**

- [Running Python Applications](../programming_guide/snappydata_jobs.md#running-python-applications)
