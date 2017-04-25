<a id="howto-odbc"></a>
# How to Connect using ODBC Driver

You can connect to SnappyData Cluster using SnappyData ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.

### Download and Install the ODBC Driver

To download and install the ODBC driver:

1. Download the SnappyData ODBC Driver from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases).  
Depending on your Windows installation, download the required version of the SnappyData ODBC Driver.

    * [For 32-bit Installer for 32-bit Platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.8/snappydata-0.8.0.1-odbc32.zip)

    * [For 32-bit Installer for 64-bit Platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.8/snappydata-0.8.0.1-odbc32_64.zip)

    * [For 64-bit Installer for 64-bit Platform](https://github.com/SnappyDataInc/snappydata/releases/download/v0.8/snappydata-0.8.0.1-odbc64.zip)

2. Extract the contents of the downloaded file. 

3. Double-click on the **SnappyDataODBCDriverInstaller.msi** file, and follow the steps to complete the installation.

For more information, refer to the documentation on [setting up SnappyData ODBC Driver and Tableau Desktop](https://github.com/SnappyDataInc/snappydata/blob/master/docs/setting_up_odbc_driver-tableau_desktop.md).

### Connect to the SnappyData cluster 
Once you have installed SnappyData ODBC Driver, you can connect to SnappyData cluster in any of the following ways:

* Use the SnappyData Driver Conneciton URL:

		Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>

* Create a SnappyData DSN (Data Source Name) using the installed SnappyData ODBC Driver.</br> 
 Please refer to the Windows documentation relevant to your operating system for more information on creating a DSN. 
 When prompted, select the SnappyData ODBC Driver from the drivers list and enter a Data Source name, SnappyData Server Host, Port, User Name and Password. 

