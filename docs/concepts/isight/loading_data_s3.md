
<a id="dataAWSS3"></a>
##Loading Data from AWS S3##
SnappyData provides you with predefined buckets which contain datasets. When data is loaded, the table reads from the files available at the specified external location (AWS S3). 


!!! Note
			
	*	The Amazon S3 buckets and files are private by default. Ensure that you set the permissions required to make the data publicly accessible. Please refer to the [documentation provided by Amazon S3](http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html) for detailed information on creating a bucket, adding files and setting required permissions.	
	* You can also find AWS related information on the AWS homepage, from the **Account** > **Security Credentials** > **Access Credentials** option.
	* Information related to the Bucket Name and Folder Location can be found on the AWS S3 site.

To define a table that references the data in AWS S3, create a paragraph in the following format:

```
%sql
DROP TABLE IF EXISTS <table_name> ;
CREATE EXTERNAL TABLE <table_name> USING parquet OPTIONS(path '<AWS_SECRET_ACCESS_KEY>:<AWS_ACCESS_KEY_ID>@<bucket_Name>/<folder_name>');
```

The values are:

**Property** | **Description/Value**
---------------|-----------------------------
```<table_name>``` |The name of the table
```<AWS_SECRET_ACCESS_KEY>:<AWS_ACCESS_KEY_ID> ```| Security credentials used to authenticate and authorize calls that you make to AWS. 
```<bucket_Name> ```| The name of the bucket where the folder is located. Default value: zeppelindemo 
```<folder_name>``` | The folder name where the data is stored. Default value: nytaxifaredata 