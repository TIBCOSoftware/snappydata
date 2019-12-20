# Accessing Cloud Data

## AWS
You can access the Public Data on AWS Cloud storage using any of the following methods:



### Using hive-site.xml File 

1.	Add the following key properties into **conf** > **hive-site.xml** file:
	*	fs.s3a.access.key
	*	fs.s3a.secret.key 

2.	Create an external table using the following command:
    		s3a://<bucketName>/<folderName> command.

### Setting the Access or Secret Key Properties

1.	Add the following properties in **conf**/**leads** file:

             -spark.hadoop.fs.s3a.access.key=<Amazon S3 Access Key>
             -spark.hadoop.fs.s3a.secret.key=<Amazon S3 Secret Key>
             
2.	Create an external table using the following command: 
			s3a://<bucketName>/<folderName> command.

### Setting the Access or Secret Key Properties through Environment Variable

Set credentials as enviroment variable:

        export AWS_ACCESS_KEY_ID=<Amazon S3 Access Key>
        export AWS_SECRET_ACCESS_KEY=<Amazon S3 Secret Key>

### Setting the Access or Secret Key Properties through Shell 

        set spark.hadoop.fs.s3a.access.key=<Amazon S3 Access Key>
        set spark.hadoop.fs.s3a.secret.key=<Amazon S3 Secret Key>

## Azure Blob

### Accessing Data from Azure Secured BLOB Storage
Setting the credentails in hive-site.xml works across all the shells.
fs.azure.account.key.your-storage_account_name.core.windows.net
CREATE TABLE testADLS1 USING CSV Options (path 'wasbs://container_name@storage_account_name.blob.core.windows.net/dir/file')

### Accessing Data from Azure without Secured BLOB Storage
CREATE TABLE testADLS1 USING CSV Options (path 'wasb://container_name@storage_account_name.blob.core.windows.net/dir/file')

### Setting Credentails through Spark Property
sc.hadoopConfiguration.set("fs.azure.account.key.<your-storage_account_name>.dfs.core.windows.net", "<secretKey>")

## GCS

### Setting Credentials in hive-site.xml 
Setting the credentials in hive-site.xml works in all the shells.
google.cloud.auth.service.account.json.keyfile

CREATE EXTERNAL TABLE airline_ext USING parquet OPTIONS(path 'gs://bucket_name/object_name ')

### Setting Credentials through Spark property on Shell 
sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile","<json file path>") . 

### Setting the Properties in lead/on shell using -spark.hadoop
-spark.hadoop.google.cloud.auth.service.account.json.keyfile=<json file path>

## EMR HDFS
Accessing data from EMR HDFS location from snappy cluster
running on ec2

create external table categories using csv options(path 'hdfs://34.230.86.97/user/hadoop/NW/categories.csv');
