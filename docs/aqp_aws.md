##Overview
iSight-Cloud is a cloud-based service that allows for instant visualization of analytic query results on large datasets. Powered by the SnappyData Synopsis Data Engine ([SDE](aqp.md)), users interact with iSight-Cloud to populate the synopsis engine with the right data sets and accelerate SQL queries by using the engine to provide latency bounded responses to large complex aggregate queries. 

iSight-Cloud uses Apache Zeppelin as the front end notebook to display results and allows users to build powerful notebooks representing key elements of their business in a matter of minutes. 

The service provides a web URL that spins up a cluster instance on AWS or users can download the iSight-Cloud EC2 script to configure a custom sized cluster, to create and render powerful visualizations of their big data sets with the click of a button. 
With iSight-Cloud, you can speed up the process of understanding what your data is telling you, and move on to the task of organizing your business around those insights rapidly.

In this document, we describe the features provided by SnappyData for analyzing your data. It also provides details for deploying a SnappyData Cloud cluster on AWS using either the CloudFormation service or by using the EC2 scripts.

Refer to the examples and guidelines provided in this document to help you create notebooks using which, you can execute SQL queries or data frame API to analyze your data.

Key Components
------------

This section provides a brief description of the key terms used in this document. 

* **Amazon Web Services (AWS**):  Amazon Web Services (AWS) is a comprehensive, evolving cloud computing platform that offers a suite of cloud-computing services. The services provided by this platform that is important for SnappyData are Amazon Elastic Compute Cloud (EC2) and Amazon Simple Storage Service (S3).

* **SnappyData Cluster**:  A database cluster which has three main components - Locator, Server and Lead

* **Apache Zeppelin**: Apache Zeppelin is a web-based notebook that enables interactive data analytics. It allows you to make data-driven, interactive and collaborative documents with SQL queries or directly use the Spark API to process data.

* **Interpreters**: A software module which is loaded into Apache Zeppelin upon startup. Interpreters allow various third party products including SnappyData to interact with Apache Zeppelin. The SnappyData interpreter gives users the ability to execute SQL queries or use the data frame API to visualize data.

Quick Start Steps
-------------


To understand the product follow these easy steps that can get you started quickly:

1. [Setting up SnappyData Cloud Cluster](concepts/isight/setting_up_cloud_cluster.md)

	* [Deploying SnappyData Cloud Cluster with iSight CloudBuilder](concepts/isight/setting_up_cloud_cluster.md#DeployingClusterCloudFormation)
	* [Deploying SnappyData Cloud Cluster on AWS using Scripts](concepts/isight/setting_up_cloud_cluster.md#DeployingClusterScript)

2. [Using Apache Zeppelin](concepts/isight/apache_zeppelin.md#LoggingZeppelin)	
	* [Using Predefined Notebook](concepts/isight/predefined_notebooks.md#predefinednotebook)
	* [Creating your own Notebook](concepts/isight/creating_notebooks.md#Creatingnotebook)

3. [Loading Data from AWS S3](concepts/isight/loading_data_s3.md#dataAWSS3)

4. [Monitoring SnappyData Cloud Cluster](concepts/isight/monitoring_cloud_cluster.md#Monitoring)
