#Overview of the Synopsis Data Engine#
Data volumes from transactional and non-transactional sources have grown exponentially, and as a result, conventional data processing and data visualization technologies have struggled to provide real time analysis on these ever increasing data sets.
Conventional wisdom has relied on iterating over the entire data set to produce complete and accurate results, while experiencing longer wait times to provide those insights. Data scientists and data engineers agree that when it comes to exploratory analytics, the ability to quickly get a directionally correct answer is more important than waiting for long periods of time to get a complete accurate answer. This is partly because in most cases, the decision to pursue a certain hypothesis does not change a whole lot whether you get back a 99% accurate answer or a 100% accurate answer.
The SnappyData Synopsis Data Engine (SDE) offers a novel and scalable solution to the data volume problem. SDE uses statistical sampling techniques and probabilistic data structures to answer aggregate class queries, without needing to store or operate over the entire data set. The approach trades off query accuracy for quicker response times, allowing for queries to be run on large data sets with meaningful and accurate error information. Using SDE, practitioners can populate the engine with synopses of large data sets, and run SQL queries to get answers orders of magnitude faster than querying the complete data set. This approach significantly reduces memory requirements, cuts down I/O by an order of magnitude, requires less CPU overall (especially when compared to dealing with large data sets) and reduces query latencies by an order of magnitude. With these improvements, users can now perform analytics at the speed of thought without being encumbered by complex infrastructure, massive data load times and long query response times.
For example, for research-based companies (like Gallup), for political polls results, a small sample is used to estimate support for a candidate within a small margin of error.
In this document, we describe the features provided by SnappyData for analysing your data. It also provides details for deploying SnappyData cluster on AWS CloudFormation or on AWS using the EC2 script. 
Refer to the the examples and guidelines provided in this document to help you create notebooks using which, you can execute SQL queries or data frame API to analyse your data.
##Key Components##
This section provides a brief description of the key terms used in this document. 

* **Amazon Web Services (AWS**): Amazon Web Services (AWS) is a comprehensive, evolving cloud computing platform that offers a suite of cloud-computing services. The services provided by this platform that are important for SnappyData are, Amazon Elastic Compute Cloud (EC2) and Amazon Simple Storage Service (S3).
* **SnappyData Cluster**: A database cluster which has three main components - Locator, Server and Lead
* **Apache Zeppelin**: Apache Zeppelin is a web-based notebook that enables interactive data analytics. It allows you to make data-driven, interactive and collaborative documents with SQL queries or directly use the Spark API to process data
* **Interpreters**: A software module which is loaded into Apache Zeppelin upon startup. Interpreters allow various third party products including SnappyData to interact with Apache Zeppelin. The SnappyData interpreter gives users the ability to execute SQL queries or use the data frame API to visualize data.

#Quick Start Steps#
Do want to understand the product and start using it in a few minutes? Follow these easy steps that can get your started quickly:
1. Setting up SnappyData Cluster
        - Deploying the Cluster with AWS CloudFormation
        - Deploying the Cluster with AWS using Script
2. Loading Data from AWS S3
3. Logging into Apache Zeppelin
        - Using Predefined Notebook
        -Creating your own Notebook

#Setting Up SnappyData Cluster#
##Prerequisites##
* Existing account with AWS with required permissions to launch EC2 resources.
* Using the Amazon Web Services (AWS) Secret Access Key and the Access Key ID, set the two environment variables, *AWS_SECRET_ACCESS_KEY* and *AWS_ACCESS_KEY_ID*.
If you already have set up the AWS Command Line Interface on your local machine, the script automatically detects and uses the credentials from the AWS credentials file.
You can also find this  information on the  AWS homepage, from the ** Account**  > **Security Credentials** > **Access Credentials** option.
For example, <code>
	>	export AWS_SECRET_ACCESS_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112
	>	export AWS_ACCESS_KEY_ID=A1B2C3D4E5F6G7H8I9J10 </code>

##Deploying the Cluster with AWS CloudFormation##
SnappyData uses the Amazon EC2 with AWS Cloud Formation method to automatically install, configure and start a SnappyData cluster. In this release, the configuration supports launching the cluster on a single EC2 instance. 
This method is recommended as the quickest way to deploy SnappyData. All you need is an existing AWS account and login credentials to get started! 
###Configuring and Launching the Cluster###
The configure and launch the cluster:
1. Go to http://www.snappydata.io/cloudbuilder. 
On this page, you need to complete the steps to configure and start the cluster.
2. Enter the name for your cluster. Each clusters is identified by it’s unique name. The names and details of the members are automatically derived from the provided cluster name.

3. Enter a name of an existing EC2 KeyPair. This enables SSH access to the cluster. Please refer to the Amazon documentation for more information on generating your own key pair.	
 
4.  Select an instance and storage based on the capacity that you need..
 
5. Currently only Amazon Elastic Block Storage (EBS) is supported by SnappyData.

6. Enter a value (between 256GB - 1024 GB) in the EBS Volume Size(gigabytes) field.

7. Click Generate. 
8. On the next page, select the AWS region, and then click Launch Cluster. 
Note: Ensure that the key pair is created in the region selected.
 
9. The Select Template page allows you to select a template that describes the stack you want to create. By default, the URL for the Amazon S3 template is provided. Click Next. 
	
10. Enter a stack name or continue to use the default value. Click Next.
11. Specify the tags  or leave the field empty to use the default values. Click Next.
12. On the Review page, verify the details and click Create to create a stack. 
	Note: This operation may take a few minutes to complete.  
13. The next page lists the existing stacks. Click Refresh to update the list and and the current status of the stack. Ensure that the status of the stack is “Create_Complete”. 
14. Open a web browser and enter the http://<Public DNS>:<Port Number> to launch Apache Zeppelin. You can find the public DNS by loading the stack’s EC2 home page.
For more information on using Notebooks, refer to Using Predefined Notebooks.	
##Deploying the Cluster on AWS using Scripts##
SnappyData provides a script that allows you to launch and manage SnappyData clusters on Amazon Elastic Compute Cloud (EC2). 
Download the script from the [SnappyData Release page.](https://github.com/SnappyDataInc/snappydata/releases/) . The package is available in compressed files (**snappy-ec2.tar**). Extract the contents to a location on your computer.
Using the snappy**-ec2** script, you can identify each of the clusters by it's unique cluster name. The script internally ties members (locators, leads and stores) of the cluster with EC2 security groups. 
###Launching SnappyData Cluster###
To execute the script,  type the following at a command prompt:
<code> ./ec2/snappy-ec2 -k <your-key-name> -i <your-keyfile-path> --snappydata-version=CUSTOM --with-zeppelin=embedded launch <your-cluster-name> </code>
The names and details of the members are automatically derived from the provided cluster name, and one instance of locator, lead and server is started.
For example, if you launch a cluster named '**my-cluster**', the locator is available in security group named 'my-cluster-locator' and the stores are available in '**my-cluster-stores**'.
When running the script you can also specify properties like number of stores and region.
For example, using the following command, you can start a SnappyData cluster named 's**nappydata-cluster**' with 4 stores (or servers) in the North Virginia region on AWS. <code>
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file --stores=4 --region=us-east-1 launch snappydata-cluster </code>
#Loading Data from AWS S3#
SnappyData provides you with predefined buckets which contain data files. When data is loaded, the table reads from the files available at the specified external location (AWS S3). 

>#####Note:

* The Amazon S3 buckets and files are private by default. Ensure that you set the permissions required to make the data publicly accessible. Please refer to the documentation provided by Amazon S3 for detailed information on creating a bucket, adding files and setting required permissions.
* You can also find AWS related information on the AWS homepage, from the Account > Security Credentials > Access Credentials option.
*Information related to the Bucket Name and Folder Location can be found on the AWS S3 site.
To define a table that references the data in AWS S3, create a paragraph in the following format:

<code>%sql
DROP TABLE IF EXISTS <table_name> ;
CREATE EXTERNAL TABLE <table_name> USING parquet OPTIONS(path '<AWS_SECRET_ACCESS_KEY>:<AWS_ACCESS_KEY_ID>@<bucket_Name>/<folder_name>');
</code>
The values are:

**Property** | **Description/Value**
---------------|-----------------------------
table_name |The name of the table
AWS_SECRET_ACCESS_KEY:AWS_ACCESS_KEY_ID | Security credentials are used to authenticate and authorize calls that you make to AWS. 
bucket_Name | The name of the bucket where the folder is located. Default value: zeppelindemo 
folder_name | The folder name where the data is stored. Default value: nytaxifaredata 


#Logging into Zeppelin#
Apache Zeppelin provides you with web-based notebooks for data exploration. A notebook consists of one or more paragraphs, and each paragraph consists of a section for code and results.
Launch Apache Zeppelin from the web browser by accessing the host and port associated with your Apache Zeppelin server. 
For example, *http://zeppelin_host:port_number*. The welcome page which lists existing notebooks is displayed.  
SnappyData provides predefined notebooks which are displayed on the home page after you have logged into Apache Zeppelin. For more information, see Using Predefined Notebooks.
##Using the Interpreter##
Snappydata Interpreter group consists of the interpreters %snappydata.snappydata and %snappydata.sql.
To use an interpreter, add the associated interpreter directive with the format %<Interpreter_name> at the beginning of a paragraph in your note. In a paragraph, use one of the interpreters, and then enter required commands.
>#####Note:
* The SnappyData Interpreter provides a basic auto-completion functionality. Press (Ctrl+.) on the keyboard to view a list of suggestions.
* It is recommend that you use the SQL interpreter to run queries on the SnappyData cluster, as an out of memory error may be reported with running the Scala interpreter.


###SQL Interpreter###
The** %snappydata.sql** code specifies the default SQL interpreter. This interpreter is used to execute SQL queries on SnappyData cluster.
####Multi-Line Statements####
Multi-line statements as well as multiple statements on the same line are also supported as long as they are separated by a semicolon. However, only the result of the last query is displayed.
For example: 
<code>%sql
set spark.sql.shuffle.partitions=6; 
select medallion,avg(trip_distance) as avgTripDist from nyctaxi group by medallion order by medallion desc limit 100 with error
</code>

####SnappyData Directives in Apache Zeppelin####
You can execute approximate queries on SnappyData cluster by using the %sql show-approx-results-first directive. 
In this case, the query is first executed on the sample table and the approximate result is displayed, after which the query is run on the base table. Once the query is complete, the approximate result is replaced with the actual result.
SnappyData provides a list of connection-specific SQL properties that can be applied to the paragraph that is executed.

For example:
<code>	%sql show-approx-results-first
	select sum(trip_time_in_secs)/60 totalTimeDrivingInHour, hour(pickup_datetime) from nyctaxi group by hour(pickup_datetime) limit 24;
</code>
###Scala Interpreter###
The **%snappydata.snappydata** code specifies the default Scala interpreter. This interpreter is used to write Scala code in the paragraph.
SnappyContext is injected in this interpreter and can be accessed using variable **snc**.
##Using Predefined Notebooks##
SnappyData provides you a predefined notebook NYCTAXI Analytics which contains definition stored in a single file. 
When you launch Apache Zeppelin in the browser, the Welcome page displays the existing notebooks. Open a notebook and run any of the paragraphs to analyse data and view the result. For more information on of the pre-defined paragraphs in the notebook, see Examples of Queries and Results.


##Creating Notebooks - Try it Yourself!##
1. Log on to Apache Zeppelin, create a notebook and insert a new paragraph.
2. Use **%snappydata.snappydata** to select SnappyData interpreter or use** %snappydata.sql** to select SQL interpreter.
3. Download a dataset you want to use. For this example, let us refer to the dataset as “employees”

If you want to see age distribution from employees, run:
<code>%sql select age, count(1) from employees where age < 20 group by age order by age </code>


#Examples of Queries and Results#
This section provides you examples you can use in a paragraph.
#Approximations Technique using Sampling#
Query processing in a database context is the process that derives information that is available in the database. 
Efficient processing of data is one of the main issues faced with query processing. In most cases, it can be expensive and time consuming for users to get an exact answer in a short response time. 

Synopsis Data Engine (SDE) provides an alternative solution that returns an approximate answer using information which is similar to the one from which the query is answered.

Sampling is when a small sample of data, which represents the entire data is randomly selected. In this case, a query is answered based on the pre-sampled small amount of data, and then scaled up based on the sample rate. 

The two techniques that the SnappyData SDE module uses to accomplish this are, reservoir sampling as applied to stratified sampling. 

Reservoir Sampling is an algorithm for sampling elements from a stream of data, where a random sample of elements are returned, which are evenly distributed from the original stream.

Stratified sampling refers to a type of sampling method where the data is divided into separate groups, called strata. Then, a simple random sample is drawn from each group. 
For more information on SDE and sampling techniques used by SnappyData, refer to http://snappydatainc.github.io/snappydata/aqp/. 
#QCS (Query Column Set) and Sample selection#

**QCS**
We term the columns used for grouping and filtering in a query (used in GROUP BY/WHERE/HAVING clauses) as the query column set or query QCS. Columns used for stratification during the sampling are termed as sample QCS. One can use functions as sample qcs column e.g.hour(pickup_datetime)

General guideline to select sample QCS is to look for columns in a table which are generally used in grouping or filtering of queries. This results in good representation of data in sample for each sub-group of data in query and approximate results will be closer actual results. 

<code>CREATE SAMPLE TABLE NYCTAXI_pickup_sample ON NYCTAXI  OPTIONS ( qcs 'hour(pickup_datetime)', fraction '0.01') AS (SELECT * FROM NYCTAXI);
</code>
You can create multiple sample tables using different sample QCS and sample fraction for a given base table. Sample selection logic selects most appropriate table on below logic.


**Sample Selection:**
1. If query QCS is exactly the same as a given sample's QCS, then that sample gets selected.
2. If exact match is not available, then, if the QCS of the sample is a superset of query QCS, that sample is used.
3. If superset of sample QCS is not available, a sample where Sample QCS is subset of Query QCS is used

When multiple stratified samples with subset of  QCSs match, sample where most number of columns match with query QCS is used. Largest size of sample gets selected if multiple such samples are available For example:. If query QCS are A, B and C. If there are samples with QCS  A&B and B&C are available then choose a sample with large sample size.

#Using Error Functions and Confidence Interval in Queries#
Acceptable error fraction and expected confidence interval can be specified in the query projection. 
A query can end with the clauses WITH ERROR and WITH ERROR <fraction> [**CONFIDENCE** <fraction>] [**BEHAVIOR** <string>] 

##Using WITH ERROR Clause##
In this clause, context level setting can be overridden by query level settings.
When this clause is specified, the query is run with the following values:
	ERROR 0.2 
	CONFIDENCE 0.95 
	BEHAVIOR 'do_nothing'
	
For example: 
<code>SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order 
by Month_ with error 
</code>
These values can be overridden by setting in the  SnappyData  context below.	
<code>
snContext.sql(s"spark.sql.aqp.error=$error")
snContext.sql(s"spark.sql.aqp.confidence=$confidence")
snContext.sql(s"set spark.sql.aqp.behavior=$behavior")
</code>
Apache Zeppelin or snappy-shell can use the set context level values as below:
<code>
set spark.sql.aqp.error=$error"
set spark.sql.aqp.confidence=$confidence
set spark.sql.aqp.behavior=$behavior; </code>
##Using WITH ERROR <fraction> [CONFIDENCE <fraction>] [BEHAVIOR <string>] Clause##
**WITH ERROR** - this is a mandatory clause. The values are  0 < value(double) < 1 . 
**CONFIDENCE** - this is optional clause. The values are confidence 0 < value(double) < 1 . The default value is 0.95
**BEHAVIOR** - this is optional clause. The values are 'do_nothing', 'local_omit','strict', 'run_on_full_table',’partial_run_on_base_table. Default value is 'run_on_full_table'
	
For example: 
<code>SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order
by Month_  with error 0.10 confidence 0.95 behavior ‘local_omit’ </code>
##HAC (High-level Accuracy Contract)##
Approximate queries have HAC support using behavior clause.  HAC recommends that the following action to be taken if the error requirement is not met.

<code>....WITH ERROR <fraction> [CONFIDENCE <fraction>] [BEHAVIOR <behavior>]</code>
HAC options supported as below:
* **do_nothing**: Report the estimate as is.
* **local_omit**: For those aggregates not satisfying the error criteria, the value is replaced by a special value. like our "null", which is different from the null ( of the null value used by the system)
* **strict**: throws exception
* **run_on_base_table**: Query will be re-executed on full table.
* **partial_run_on_base_table**: Query will be re-executed on base/full table for only those sub-groups where error is more than the specified in query. These results table will be merged with rest of the result from sample table. 
##Enhanced DataFrame API to support behavior clause#
<code>
def withError(error: Double,
confidence: Double = Constant.DEFAULT_CONFIDENCE,
behavior: String = "DO_NOTHING"): DataFrame
</code>