# Working with Stratified Samples


## Create Sample Tables

You can create sample tables on datasets that can be sourced from any source supported in Spark/TIBCO ComputeDB. For instance, these can be TIBCO ComputeDB in-memory tables, Spark DataFrames, or sourced from an external data source such as S3 or HDFS.
Creation of sample table will implicitly sample the data from the base table.

Here is an SQL based example to create a sample on tables locally available in the TIBCO ComputeDB cluster. 

```pre
CREATE SAMPLE TABLE NYCTAXI_PICKUP_SAMPLE ON NYCTAXI 
  OPTIONS (qcs 'hour(pickup_datetime)', fraction '0.01'); 


CREATE SAMPLE TABLE TAXIFARE_HACK_LICENSE_SAMPLE on TAXIFARE 
  OPTIONS (qcs 'hack_license', fraction '0.01');
```

Often your data set is too large to also fit in available cluster memory. If so, you can create an external table pointing to the source. 
In this example below, a sample table is created for an S3 (external) dataset:

```pre
CREATE EXTERNAL TABLE TAXIFARE USING parquet 
  OPTIONS(path 's3a://<AWS_SECRET_ACCESS_KEY>:<AWS_ACCESS_KEY_ID>@zeppelindemo/nyctaxifaredata_cleaned');
//Next, create the sample sourced from this table ..
CREATE SAMPLE TABLE TAXIFARE_HACK_LICENSE_SAMPLE on TAXIFARE 
  options  (qcs 'hack_license', fraction '0.01');
```

When creating a base table, if you have applied the **partition by** clause, the clause is also applied to the sample table. The sample table also inherits the **number of buckets**, **redundancy** and **persistence** properties from the base table.

For sample tables, the **overflow** property is set to **False** by default. (For row and column tables the default value is  **True**). 

For example:

```pre
CREATE TABLE BASETABLENAME <column details> 
USING COLUMN OPTIONS (partition_by '<column_name_a>', Buckets '7', Redundancy '1')

CREATE TABLE SAMPLETABLENAME <column details> 
USING COLUMN_SAMPLE OPTIONS (qcs '<column_name_b>',fraction '0.05', 
strataReservoirSize '50', baseTable 'baseTableName')
// In this case, sample table 'sampleTableName' is partitioned by column 'column_name_a', has 7 buckets and 1 redundancy.
```


!!! Note
	* After a sample table is created from a base table, any changes to the base table, (for example update and delete operations) is not automatically applied to the sample table.
	* Howerver, if the data is populated in the main table using any of the following ways:
		*	dataFrame.write
		*	Using JDBC API batch insert
		*	Insert into table values select * from x
Then the data is also sampled and put into sample table(s) if exists.
    
    * For successful creation of sample tables, the number of buckets in the sample table should be more than the number of nodes in the cluster. 

## QCS (Query Column Set) and Sample Selection
For stratified samples, you are required to specify the columns used for stratification(QCS) and how big the sample needs to be (fraction). 

QCS, which stands for Query Column Set is typically the most commonly used dimensions in your query GroupBy/Where and Having clauses. A QCS can also be constructed using SQL expressions - for instance, using a function like `hour (pickup_datetime)`.

The parameter *fraction* represents the fraction of the full population that is managed in the sample. Intuition tells us that higher the fraction, more accurate the answers. But, interestingly, with large data volumes, you can get pretty accurate answers with a very small fraction. With most data sets that follow a normal distribution, the error rate for aggregations exponentially drops with the fraction. So, at some point, doubling the fraction does not drop the error rate. SDE always attempts to adjust its sampling rate for each stratum so that there is enough representation for all sub-groups. 
For instance, in the above example, taxi drivers that have very few records may actually be sampled at a rate much higher than 1% while very active drivers (a lot of records) is automatically sampled at a lower rate. The algorithm always attempts to maintain the overall 1% fraction specified in the 'create sample' statement. 

One can create multiple sample tables using different sample QCS and sample fraction for a given base table. 

Here are some general guidelines to use when creating samples:

* Note that samples are only applicable when running aggregation queries. For point lookups or selective queries, the engine automatically rejects all samples and runs the query on the base table. These queries typically would execute optimally anyway on the underlying data store.

* Start by identifying the most common columns used in GroupBy/Where and Having clauses. 

* Then, identify a subset of these columns where the cardinality is not too large. For instance, in the example above 'hack_license' is picked (one license per driver) as the strata and 1% of the records associated with each driver is sampled. 

* Avoid using unique columns or timestamps for your QCS. For instance, in the example above, 'pickup_datetime' is a time stamp and is not a good candidate given its likely hood of high cardinality. That is, there is a possibility that each record in the dataset has a different timesstamp. Instead, when dealing with time series the 'hour' function is used to capture data for each hour. 

* When the accuracy of queries is not acceptable, add more samples using the common columns used in GroupBy/Where clauses as mentioned above. The system automatically picks the appropriate sample. 

!!! Note
	The value of the QCS column should not be empty or set to null for stratified sampling, or an error may be reported when the query is executed.

