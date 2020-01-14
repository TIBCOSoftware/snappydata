# Accessing Cloud Data

## AWS
To access the data on AWS cloud storage, you must set the credentials using any of the following methods:

*	Using hive-site.xml File
*	Setting the Access or Secret Key Properties
*	Setting the Access or Secret Key Properties through Environment Variable

### Using hive-site.xml File 

Add the following key properties into **conf** > **hive-site.xml** file:

```
   <property>
       <name>fs.s3a.access.key</name>
       <value>Amazon S3 Access Key</value>
   </property>
   <property>
       <name>fs.s3a.secret.key</name>
       <value>Amazon S3 Secret Key</value>
   </property>
```
   
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

### Accessing Data from AWS Cloud Storage

Create an external table using the following command:

```
create external table staging_parquet using parquet options (path 's3a://<bucketName>/<folderName>');
create table parquet_test using column as select * from staging_parquet;
```

### Unsetting the Access or Secret Key Properties

You can run the **org.apache.hadoop.fs.FileSystem.closeAll()** command on the snappy-scala shell or in the job. This clears the cache. Ensure that there are no queries running on the cluster when you are executing this property.  After this  you can set the new credentials. 

## Azure Blob

To access the data on Azure Blob storage, you must set the credentials using any of the following methods:

*	Setting Credentials through hive-site.xml
*	Setting Credentials through Spark Property

###  Setting Credentials through hive-site.xml

Set the following property in hive-site.xml

```
	<property> 
    	<name>fs.azure.account.key.youraccount.blob.core.windows.net</name> 
    	<value>YOUR ACCESS KEY</value>
    </property>
```

### Setting Credentials through Spark Property 


```
sc.hadoopConfiguration.set("fs.azure.account.key.<your-storage_account_name>.dfs.core.windows.net", "<YOUR ACCESS KEY>")
```

### Accessing Data from Azure Unsecured BLOB Storage

```
    CREATE EXTERNAL TABLE testADLS1 USING PARQUET Options (path 'wasb://container_name@storage_account_name.blob.core.windows.net/dir/file')
```

### Accessing Data from Azure Secured BLOB Storage

```
    CREATE EXTERNAL TABLE testADLS1 USING PARQUET Options (path 'wasbs://container_name@storage_account_name.blob.core.windows.net/dir/file')
```

## GCS

To access the data on GCS cloud storage, you must set the credentials using any of the following methods:

*	Setting Credentials through hive-site.xml
*	Setting Credentials through Spark Property

### Setting Credentials in hive-site.xml 
```

   <property>
       <name>google.cloud.auth.service.account.json.keyfile</name>
       <value>/path/to/keyfile</value>
   </property>

```

### Setting Credentials through Spark property on Shell 

```
sc.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile","`<json file path>`")
``` 

### Accessing Data from GCS Cloud Storage

```
CREATE EXTERNAL TABLE airline_ext USING parquet OPTIONS(path 'gs://bucket_name/object_name')
```

## EMR HDFS 

You can use the following command to access data from EMR HDFS location for a cluster that is running on ec2:

```
create external table categories using csv options(path 'hdfs://<masternode IP address>/<file_path>');
```

