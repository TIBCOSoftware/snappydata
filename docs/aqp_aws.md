Overview of iSight (AQP) {.P119}
========================

Development in data collection and management technologies has lead to
data volumes from transactional and non-transactional sources to grow
exponentially.

These data repositories through analysis can provide insights related to
the data, and the processes that generated the data. When a large
database is queried, it is important that the query response time is
kept short, while the accuracy of the query results can be considered
less important.

Approximate query processing (AQP) offers an exponential solution to the
data volume problem. The basic idea behind approximate query processing
is that one can use statistical sampling techniques and probabilistic
data structures to answer aggregate class queries, without needing to
store or operate over the entire data set. The approach trades off query
accuracy for quicker response times, allowing for queries to be run on
large data sets with meaningful and accurate error information.  

For example, for research-based companies (like Gallup), for political
polls results, a small sample is used to estimate support for a
candidate within a small margin of error.

Key Components {.P110}
--------------

-   •Amazon Web Services (AWS): Amazon Web Services (AWS) is a
    comprehensive, evolving cloud computing platform that offers a suite
    of cloud-computing services. The  services provided by this platform
    that are important for iSight are, Amazon Elastic Compute Cloud
    (EC2) and Amazon Simple Storage Service (S3).  

-   •SnappyData Cluster: A database server cluster which has three main
    components - Locator, Server and Lead  

-   •Apache Zeppelin: Apache Zeppelin is a web-based notebook that
    enables interactive data analytics. It allows you to make
    data-driven, interactive and collaborative documents with SQL and
    SnappyData queries. 

-   •Interpreters: Zeppelin is used to retrieve data using the
    SnappyData interpreter. It provides the flexibility to execute any
    Scala code and SQL queries.  

Setting Up SnappyData Cluster {.P120}



Single-node Cluster Setup on AWS (Cloud-formation) {.Heading_20_2}
--------------------------------------------------

\<To be done description and steps. Working with Hemant\>

Multi-node Cluster Setup on Amazon Web Services (AWS) {.P111}
-----------------------------------------------------

SnappyData provides a script that allows you to launch and manage
SnappyData clusters on Amazon Elastic Compute Cloud (EC2).

Download the script from \<To be done name and location\>.

### Prerequisites {.P121}

-   •Existing account with AWS with required permissions to launch EC2
    resources. 

-   •Using the Amazon Web Services (AWS) Secret Access Key and the
    Access Key ID, set the two environment variables,
    AWS\_SECRET\_ACCESS\_KEY and AWS\_ACCESS\_KEY\_ID . 

    If you already have set up the AWS Command Line Interface on your
    local machine, the script automatically detects and uses the
    credentials from the AWS credentials file. 

    You can also find this  information on the  AWS homepage, from the
    Account \> Security Credentials \> Access Credentials option.  

 For example,

        export
AWS\_SECRET\_ACCESS\_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112

 export AWS\_ACCESS\_KEY\_ID=A1B2C3D4E5F6G7H8I9J10

### Launching SnappyData Cluster {.P121}

Using the \<To be done script name\> script, you can identify each of
the clusters by it's unique cluster name. The script internally ties
members (locators, leads and stores) of the cluster with EC2 security
groups. The names and details of the members are automatically derived
from the provided cluster name.

For example, if you launch a cluster named 'my-cluster', the locator is
available in security group named 'my-cluster-locator' and the stores
are available in 'my-cluster-stores'.

When you run the \<to be done name of script\> script, by default, it
starts one instance of locator, lead and server.

The following command provides an example of starting a snappydata
cluster named 'snappydata-cluster' with 4 stores (or servers) in North
Virginia region in AWS.

./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file
--stores=4 --region=us-east-1 launch snappydata-cluster

Note:  You can also configure each member in a cluster individually. For more
information, see [How can I configure each member (lead, locator,
server) in a cluster individually?](#__RefHeading___Toc921_99627442)

Local Machine Setup {.Heading_20_2}
-------------------

### Prerequisites {.Heading_20_3}

-   •Download the latest
    [SnappyData](https://github.com/SnappyDataInc/snappydata/releases/)[source
    or
    binary](https://github.com/SnappyDataInc/snappydata/releases/)from
    the release page, which lists the latest and previous releases of
    SnappyData. The packages are available in compressed files (.zip and
    .tar format). Extract the contents of the compressed file to a
    location on your computer. 

-   •\
    [ANNOTATION:\
    \
    BY 'shyja '\
    ON '2016-09-12T19:07:45'\
    NOTE: 'Is this step optional?']Before you begin, ensure that
    Zeppelin is installed. For more information on installation, refer
    to the Zeppelin documentation.  
-   •All systems in the cluster, including the system on which Zeppelin
    is installed, must be accessible at all times.  

-   •The user should have a moderate level of knowledge in using
    Zeppelin. 

### Step 1: Configuring SnappyData Cluster {.Heading_20_3}

The locator, server and lead can be configured individually using
configuration files.  These files contain the hostnames of the nodes
where you intend to start the member, followed by the configuration
properties for that member.

Template files available in the /conf directory provide startup
configuration parameters.

The following rules apply when modifying the configuration files:

-   •Provide the hostnames of the nodes where you intend to start the
    member 

-   •Ensure that one node name is provided per line 

#### Configuring Locators {.Heading_20_4}

Locators provide discovery service for the cluster. It informs a new
member joining the group about other existing members. A cluster usually
has more than one locator for high availability reasons.

In this file, you can specify:

-   •The host name on which a SnappyData lead is started  

-   •The startup directory where the logs and configuration files for
    that lead instance are located  

-   •SnappyData specific properties that can be passed 

Create the configuration files (conf/locators) for locators in
\`SNAPPY\_HOME\`.

#### Configuring Leads {.Heading_20_4}

Lead Nodes act as a Spark driver by maintaining a singleton
SparkContext. There is one primary lead node at any given instance, but
there can be multiple secondary lead node instances on standby for fault
tolerance. The lead node hosts a REST server to accept and run
applications. The lead node also executes SQL queries routed to it by
“data server” members.

Enable SnappyData Zeppelin interpreter by setting
zeppelin.interpreter.enable to true in Lead node configuration.
\<Command→ to be done\>

Create the configuration files (conf/leads) for leads in
\`SNAPPY\_HOME\`.

#### Configuring Data Servers {.Heading_20_4}

Data Servers hosts data, embeds a Spark executor, and also contains a
SQL engine capable of executing certain queries independently and more
efficiently than Spark. Data servers use intelligent query routing to
either execute the query directly on the node, or pass it to the lead
node for execution by Spark SQL.

#### Starting the SnappyData Cluster {.P61}

Navigate to the /snappy/root directory. The start script starts up a
minimal set of essential components to form the cluster, they are, one
locator, one data server and one lead node. All nodes are started
locally.

Run the start script

./sbin/snappy-start-all.sh

Note:

-   •If the directory and properties are not specified, a default
    directory is created inside the SPARK\_HOME directory. 

-   •Ensure that you repeat the configuration steps explained in this
    section, on each computer in the cluster.  

### Step 2: Installing Zeppelin Service {.Heading_20_3}

Zeppelin interpreter allows the SnappyData interpreter to be plugged
into Zeppelin, using which, you can run queries.

1.  1.\
    [ANNOTATION:\
    \
    BY 'shyja '\
    ON '2016-09-12T19:10:35'\
    NOTE: 'These steps are incorrect and needs to be verified. ']Extract
    the snappydata-interpreter.tar.gz file.  

-   -   ◦Copy the snappydatasql directory and its contents from
        snappydata-interpreter to \<ZEPPELIN\_HOME\>/interpreter/ 

    -   ◦Copy the zeppelin-site.xml file from snappydata-interpreter to
        \<ZEPPELIN\_HOME\>/conf/ 

    -   ◦Copy the
        snappydata-assembly\_2.10-0.4.0-PREVIEW-hadoop2.4.1.jar file to
        the \<ZEPPELIN\_HOME\>/interpreter/snappydatasql directory 

1.  2.Restart the Zeppelin daemon using the command
    bin/zeppelin-daemon.sh start 

2.  3.To ensure that the installation is successful, visit
    http://localhost:8080 from your web browser. 

### Step 3: Configuring SnappyData for Apache Zeppelin {.P122}

To configure SnappyData on Zeppelin, you need to do the following:

1.  1.Log on to Zeppelin from your web browser and select Interpretor
    from the Settings option.  

2.  2.In the Search Interpretor box, enter SnappyData and click Search.
     

3.  3.SnappyData is displayed in the search results.  

-   •Configure Zeppelin to connect to the remote interpretor: Select the
    Connect to existing process option, to configure Zeppelin to connect
    to the remote interpretor.  

  ---------- ----------- ---------------------------------------------------------
  Property   Value       Description
  Host       localhost   Specify host on which snappydata lead node is executing
  Port       3768        Specify the Zeppelin server port
  ---------- ----------- ---------------------------------------------------------

-   •Configure the interpreter properties: Click Edit to configure the
    settings required for the SnappyData interpreter. You can edit other
    properties if required, and then click Save to apply your changes.  

 The table lists the properties required for SnappyData.

  ------------------------------ ----------------------------------------- -------------------------------------------------------------
  Property                       Value                                     Description

  default.url                    jdbc:snappydata://localhost:1527/         Specify the JDBC URL for SnappyData cluster in the format
                                                                           
                                                                           jdbc:snappydata://\<locator\_hostname\>:1527
                                                                           
                                                                            

  default.driver                 com.pivotal.gemfirexd.jdbc.ClientDriver   Specify the JDBC driver for SnappyData

  snappydata.store.locators      localhost:10334                           Specify the URI of the locator (only local/split mode)

  master                         local[\*]                                 Specify the URI of the spark master (only local/split mode)

  zeppelin.jdbc.concurrent.use   true                                      Specify the Zeppelin scheduler to be used.
                                                                           
                                                                           Select True for Fair and False for FIFO
  ------------------------------ ----------------------------------------- -------------------------------------------------------------

-   •Bind the interpreter: You can click on the interpreter to bind or
    unbind it. Click on the interpreter from the list to bind it to the
    notebook. Click Save to apply your changes. 

-   •Set SnappyData as the default interpreter: Click and drag
    snappydata %snappydata (default), %sql to the top of the list to set
    it as the default interpreter. Click Save to apply your changes. 

Using iSight (AQP) {.P113}
------------------

Launch Zeppelin from the web browser by accessing the host and port
associated with your Zeppelin server. For example,
[http://](http://localhost:8080/)[\<zeppelin\_host\>](http://localhost:8080/)[:](http://localhost:8080/)[\<](http://localhost:8080/)port\_number\>.

The welcome page which lists existing notebooks is displayed.  

### Working with Notebooks {.P123}

Zeppelin provides you with web-based notebooks for data exploration. A
note consists of one or more paragraphs, and each paragraph consists of
a section for code and results.

To create a note click on the link provided on the Welcome page, or
select Create new note from the Notebook drop-down menu. The new note is
listed on the welcome page.

 

![](data:image/*;base64,iVBORw0KGgoAAAANSUhEUgAAABgAAAAWCAYAAADafVyIAAABfElEQVR4nOWVL0xCURSHv8CjPArlUShYKFBMECRr0WJQiwTIGoANdSOBGxKIbuAGGjRIAAMGKBYoWiBAwfIIWiA8yiPoe8KmbsieIJvTX7i75/4539nZb/eaCoXCCwuUSR+8Xu9CktdqtRFgkfoPAKVD8STJdafPcKgvqAwED4mzCG7zvAClQS6d57ZhZf/8FI9FX6sTDRQZqNp8LkCvTjp5x3JwE0ez+HlPlalkk1QU9S0023zsBn3YjANkctE41a5A/aDOAA8742p77XvkYZ9+SyJxtIakPhAPlXnc1gAWowBFpt13cXh5PGrLqGw65TixzBOe8B7P5SGS3YZNsSJOaZVBFym0MiHCJVhPpAg6msTKPUM3jQHUNvlSF0EUqcYDVBGQViKIPwfQ/Cm6iGQ/tsyYJgPMIlaaJANbYydq3h842fhe7mkAN5Grm/d47P1Z9AueCl1mCaddJq21bJIE+yrSF1Y1CFjCn7rAb+jwLIA59EcA+t+5KL0C5Qx46/YBeVMAAAAASUVORK5CYII=)

 

### Using the Zeppelin Interpreter {.P124}

Snappydata Interpreter group consists of the interpreters
%snappydata.snappydata and %snappydata.sql.

To use an interpreter, add the associated interpreter directive with the
format %\<Interpreter\_name\> at the beginning of a paragraph in your
note. In a paragraph, use one of the interpreters, and then enter
required commands.

Note:

-   •The SnappyData Interpreter provides a basic auto-completion
    functionality. Press (Ctrl+.) on the keyboard to view a list of
    suggestions. 

-   •It is recommend that you use the SQL interpreter to run queries on
    the SnappyData cluster, as an out of memory error may be reported
    with running the Scala interpreter. 

#### SQL Interpreter {.P62}

The %snappydata.sql code specifies the default SQL interpreter. This
interpreter is used to execute SQL queries on SnappyData cluster.

Multi-line statements as well as multiple statements on the same line
are also supported as long as they are separated by a semi-colon.
However, only the result of the last query is displayed.

For example: \<To be done\>

You can execute approximate queries on SnappyData cluster by using the
%sql show-approx-results-first directive. In this case, the query is
first executed on the sample table and the approximate result is
displayed, after which the query is run on the base table. Once the
query is complete, the approximate result is replaced with the actual
result.

SnappyData provides a list of connection-specific SQL properties that
can be applied to the paragraph that is executed.

For example:

If user specifies a clause then query will run with the following
values:

WITH ERROR 0.2

CONFIDENCE 0.95

BEHAVIOR 'do\_nothing'

 

These values can be overridden by setting in the snappy context below:

snContext.sql(s"spark.sql.aqp.withError=\$error")

snContext.sql(s"spark.sql.aqp.confidence=\$confidence")

snContext.sql(s"set spark.sql.aqp.behavior=\$behavior")

#### Scala Interpreter {.Heading_20_4}

The %snappydata.snappydata code specifies the default Scala interpreter.
This interpreter is used to write Scala code in the paragraph.

SnappyContext is injected in this interpreter and can be accessed using
variable snc

### Examples of Queries and Results {.P125}

This section provides you a non-exhaustive list of queries that you can
use in a paragraph.

To be Done- Will contain screenshots of the notebook and query output

 

### Using Error Functions and Confidence Interval in Queries {.Heading_20_3}

\
[ANNOTATION:\
\
BY 'shyja '\
ON '2016-09-12T19:17:11'\
NOTE: 'TO BE DONE']Acceptable error fraction and expected confidence
interval
[http://snappydatainc.github.io/snappydata/aqp/](http://snappydatainc.github.io/snappydata/aqp/)

SnappyData supports error functions that can be specified in the query
projection

 

 

 

 

Frequently Asked Questions {.P114}
--------------------------

### How can I configure each member (lead, locator, server) in a cluster individually? {.Heading_20_3}

When launching SnappyData Cluster on AWS, you can configure each of the
locators, leads or servers with specific properties.

This is done by modifying the files named locators, leads or servers,
and placing them under
product\_dir/ec2/deploy/home/ec2-user/snappydata/.

Ensure that you enter the number for each member.

For example, {{LOCATOR\_N}}, {{LEAD\_N}} or {{SERVER\_N}} in their
respective files. The script replaces these with the actual hostname of
the members when they are launched.

The sample configuration files for a cluster with 2 locators, 1 lead and
2 stores are given below.

locators

{{LOCATOR\_0}} -peer-discovery-port=9999 -heap-size=1024m

{{LOCATOR\_1}} -peer-discovery-port=9888 -heap-size=1024m

leads

{{LEAD\_0}} -heap-size=4096m -J-XX:MaxPermSize=512m -spark.ui.port=3333
-locators={{LOCATOR\_0}}:9999,{{LOCATOR\_1}}:9888
-spark.executor.cores=10

servers

{{SERVER\_0}} -heap-size=4096m
-locators={{LOCATOR\_0}}:9999,{{LOCATOR\_1}}:9888

{{SERVER\_1}} -heap-size=4096m
-locators={{LOCATOR\_0}}:9999,{{LOCATOR\_1}}:9888 -client-port=1530

When you run the \<to be done name of the script\> script, it looks for
these files under ec2/deploy/home/ec2-user/snappydata/ and, if present,
reads them while launching the cluster on Amazon EC2.

Note: Ensure that the number of locators, leads or servers specified in
the script for options, --locators, --leads or --stores match to the
number of entries in the respective files.

For example:

The script also reads snappy-env.sh \<to be done, what is this script\>,
if present in this location.

### How do I upload my data to S3 on AWS and use it to run queries using the SnappyData interpreter? {.P126}

To use Amazon S3, you need an AWS account.

Note: Please refer to the documentation provided by Amazon S3 for
detailed information on creating a bucket, adding files and setting
required permissions.

The Amazon S3 buckets and files are private by default. Ensure that you
set the permissions required to make the data publicly accessible.

Step 1: Upload Data to S3

To upload your data to S3:

1.  1.Login to the AWS Management Console and then from the Services
    drop-down select, S3. 

2.  2.Select an existing bucket or click Create Bucket to create a
    bucket.  

To create a bucket, enter the following information in the Create a
Bucket dialog box. In the Bucket Name field, enter a bucket name. Select
a region from the Region drop-down list.

Click Create.

1.  3.Select the bucket in which you want to upload the file, and click
    Upload. 

2.  4.In the Upload - Select Files wizard, click Add Files. 

3.  5.Browse and select the file that you want to upload, and then click
    Open. 

4.  6.Click Start Upload. The progress of the upload is displayed. 

Step 2: Using AWS S3 data from Notebooks

To map the data in Zeppelin:

1.  1.Log on to Zeppelin from your web browser. 

2.  2.Create a new paragraph, or update an existing paragraph by adding
    the following: 

CREATE EXTERNAL TABLE NYCTAXI USING parquet OPTIONS(path
'AWS\_SECRET\_ACCESS\_KEY+AWS\_ACCESS\_KEY\_ID@\<Bucket\_Name\>/\<Folder\_Location\>');

 

Note:

-   •You can also find AWS related information on the AWS homepage, from
    the Account \> Security Credentials \> Access Credentials option. 

-   •Information related to the Bucket Name and Folder Location can be
    found on the AWS S3 site. 

 

### \
[ANNOTATION:\
\
BY 'shyja '\
ON '2016-09-12T19:19:54'\
NOTE: 'To Be Done']How do I create samples? {.Heading_20_3}
