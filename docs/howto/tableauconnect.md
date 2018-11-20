# How to Connect Tableau to SnappyData

To connect Tableau to SnappyData, you must download and install SnappyData Enterprise Version 1.0.2.1.
Use the following steps to connect Tableau to SnappyData:

## Step 1: Enable Hive Server in SnappyData Cluster
1. Download and Install the SnappyData Enterprise version 1.0.2.1 from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases). 
2.	[Configure the SnappyData Cluster](../configuring_cluster/configuring_cluster.md).
3.	In the [Lead node configuration](../configuring_cluster/configuring_cluster.md#configuring-leads), set the following property:</br>`snappydata.hiveServer.enabled=true`
4.	Launch the SnappyData cluster. </br>`./sbin/snappy-start-all.sh`

##Step 2: Connect to Tableau Desktop
1.	Download and install **Tableau Desktop v2018.3.x** from the [Tableau Download
page](https://www.tableau.com/support/releases/online/2018.3). You may also need to register your product.
2. Open the Tableau Desktop application, on the left panel, from the **To A Server **section, select **Spark SQL connector** option. 
	![Tableau_desktop](../Images/LocateSparkSQL.png)
3. In the Spark SQL SQL configuration dialogue box, enter the following details:

	![Tableau_desktop](../Images/SparkSQL_ConfigWindow1.png)
    
	*	Enter the host and port of HiveThrift Server running in lead node of SnappyData Cluster.
	*	Select SparkThriftServer option from Type dropdown.
	*	Select **username** option from  the **Authentication** dropdown.
	*	Set **Transport** field to **SASL**.
	*	Provide a username in **Username** field.
	
    !!! Note
		For more information about Spark SQL configurations, click [here](https://onlinehelp.tableau.com/current/pro/desktop/en-us/examples_sparksql.htm).

4. Click the **Sign In **button to connect to SnappyData. Tableau displays the page where you can browse and select Schema and Tables as per your requirements to create data visualizations.

	!!! Note
    	The **Sign In** button is disabled, if Simba Spark ODBC Driver is not already installed on your system.  To enable it, click the **Download and Install the drivers **link and install Simba Spark ODBC Driver. Now the **Sign in** button is enabled.





