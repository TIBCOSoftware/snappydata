<a id="howto-odbc"></a>
## How to Connect using ODBC Driver

You can connect to SnappyData Cluster using SnappyData ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.
<a id="howto-odbc-step1"></a>
### Step 1: Install Visual C++ Redistributable for Visual Studio 2015 

To download and install the Visual C++ Redistributable for Visual Studio 2015:

1. [Download Visual C++ Redistributable for Visual Studio 2015](https://www.microsoft.com/en-in/download/details.aspx?id=48145)

2. Depending on your Windows installation, download the required version of the SnappyData ODBC Driver.

3. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
### Step 2: Install SnappyData ODBC Driver

To download and install the ODBC driver:

1. Download the SnappyData ODBC Driver from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases).

2. Depending on your Windows installation, download the 32-bit or 64-bit version of the SnappyData ODBC Driver.

	* [32-bit for 32-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc32.zip)

	* [32-bit for 64-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc32_64.zip) 

	* [64-bit for 64-bit platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.9/snappydata-0.9-odbc64.zip) 

3. Extract the contents of the downloaded file.

4. Double-click on the **SnappyDataODBCDriverInstaller.msi** file, and follow the steps to complete the installation.

	!!! Note: 
		Ensure that [SnappyData version 0.8 or later is installed](http://snappydatainc.github.io/snappydata/install/) and the [SnappyData cluster is running](../howto.md#howto-startCluster).

### Connect to the SnappyData cluster 
Once you have installed SnappyData ODBC Driver, you can connect to SnappyData cluster in any of the following ways:

* Use the SnappyData Driver Connection URL:

		Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>

* Create a SnappyData DSN (Data Source Name) using the installed SnappyData ODBC Driver.</br> 

 Please refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>When prompted, select the SnappyData ODBC Driver from the driver's list and enter a Data Source name, SnappyData Server Host, Port, User Name and Password. 

Refer to the documentation for detailed information on [Setting Up SnappyData ODBC Driver and Tableau Desktop](../setting_up_odbc_driver-tableau_desktop.md).  
