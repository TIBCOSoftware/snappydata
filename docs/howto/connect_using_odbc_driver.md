<a id="howto-odbc"></a>
# How to Connect using ODBC Driver

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

You can connect to SnappyData Cluster using SnappyData ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.

<a id="howto-odbc-step1"></a>
## Step 1: Install Visual C++ Redistributable for Visual Studio 2013

To download and install the Visual C++ Redistributable for Visual Studio 2013:

1. [Download Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784)

2. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
## Step 2: Install SnappyData ODBC Driver

To download and install the ODBC driver:

1. [Download the SnappyData 1.0.2.1 Enterprise Version](http://www.snappydata.io/download) by registering on the SnappyData website. The downloaded file contains the SnappyData ODBC driver installers.

2. Depending on your Windows installation, extract the contents of the 32-bit or 64-bit version of the SnappyData ODBC Driver.

    | Version | ODBC Driver |
    |--------|--------|
    |32-bit for 32-bit platform|snappydata-1.0.0-odbc32.zip|
    |32-bit for 64-bit platform|snappydata-1.0.0-odbc32_64.zip|
    |64-bit for 64-bit platform|snappydata-1.0.0-odbc64.zip|

4. Double-click on the **SnappyDataODBCDriverInstaller.msi** file, and follow the steps to complete the installation.

	!!! Note
		Ensure that [SnappyData is installed](../install.md) and the [SnappyData cluster is running](start_snappy_cluster.md).

## Connect to the SnappyData Cluster 
Once you have installed the SnappyData ODBC Driver, you can connect to SnappyData cluster in any of the following ways:

* Use the SnappyData Driver Connection URL:

		Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>

* Create a SnappyData DSN (Data Source Name) using the installed SnappyData ODBC Driver. Refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>
When prompted, select the SnappyData ODBC Driver from the list of drivers and enter a Data Source name, SnappyData Server Host, Port, User Name and Password.
Refer to the documentation for detailed information on [Setting Up SnappyData ODBC Driver](../setting_up_odbc_driver-tableau_desktop.md).  
