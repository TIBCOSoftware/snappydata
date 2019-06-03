# How to Connect TIBCO® Data Virtualization to TIBCO ComputeDB

TIBCO® Data Virtualization integrates disparate data sources in real-time instead of copying their data into a data warehouse. 

Do the following to connect TIBCO Data Virtulization (TDV) to TIBCO ComputeDB:

*	[Create a TDV Data Source Adapter](#create_datasource_tdv)
*	[Copy the JDBC Driver to the Adapter location](#copyjdbcdriver)
*	[Create a TDV Data Source for TIBCO ComputeDB instance](#createtcdbinstance)

<a id= create_datasource_tdv> </a>
## Create a TDV Data Source Adapter

1.	In the TDV Studio go to **File** > **New** > **Data** **Source**.
2.	Select **Custom** **Adapter** >** New Adapter** and enter the following details as mentioned:
	*	Name: **TIBCO ComputeDB**
	*	Parent Adapter: **Apache Hive 2.x**
	*	Adapter Location: The location must default in a similar pattern to **C:/Program Files/TIBCO/TDV Server 8.1/conf/adapters/custom/tibco_computedb**
	*	Adapter Class Name: **io.snappydata.jdbc.ClientDriver**
	*	Connection URL Pattern: **jdbc:snappydata://<HOST>:<PORT>**

<a id= copyjdbcdriver> </a>
## Copy the JDBC Driver

1.	Download the JDBC driver from [here](https://github.com/SnappyDataInc/snappydata/releases/tag/v1.1.0). The JDBC driver is available as **snappydata-jdbc_2.11-1.1.0.jar**.
2.	Copy the jar file to the Adapter location that is specified while creating the TDV Data Source Adapter. This location must default in a similar pattern to **C:/Program Files/TIBCO/TDV Server 8.1/conf/adapters/custom/tibco_computedb**.
3.	Restart the TDV Server to load the JDBC driver.

<a id= createtcdbinstance> </a>
## Create a TDV Data Source for TIBCO ComputeDB Instance

1.	In the TDV Studio,  go to **File** > **New** > **Data Source**.
2.	Find **TIBCO ComputeDB** and select **Next**.
3.	Enter the following details:
	*	Datasource Name: Name that is used to identify the instance of TIBCO ComputeDB.
	*	Host: URL of the server location.
	*	Port: usually 1527 or 1528 
	*	Database Name: This can be left blank.
	*	Login and Password details.
4.	Select **Create and Introspect**.
5.	Select the required tables and then click **Next**.
6.	Click **Finished**.