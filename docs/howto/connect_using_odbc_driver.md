<a id="howto-odbc"></a>
# How to Connect using ODBC Driver

You can connect to TIBCO ComputeDB Cluster using TIBCO ComputeDB ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.

<a id="howto-odbc-step1"></a>
## Step 1: Installing Visual C++ Redistributable for Visual Studio 2013

To download and install the Visual C++ Redistributable for Visual Studio 2013:

1. [Download Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784)

2. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
## Step 2: Installing TIBCO ComputeDB ODBC Driver

To download and install the ODBC driver:

1. Download the drivers zip file **TIB_compute_drivers_1.2.0_linux.zip** using the steps provided [here](/quickstart/getting_started_by_installing_snappydata_on-premise.md). After this file is  extracted, you will find that it contains the ODBC installers in another file **TIB_compute-odbc_1.2.0_win.zip**. 
2. Extract **TIB_compute-odbc_1.2.0_win.zip**. Depending on your Windows installation, extract the contents of the 32-bit or 64-bit version of the TIBCO ComputeDB ODBC Driver.

    | Version | ODBC Driver |
    |--------|--------|
    |32-bit for 32-bit platform|TIB_compute-odbc_1.2.0_win_x86.zip|
    |64-bit for 64-bit platform|TIB_compute-odbc_1.2.0_win_x64.zip|

4. Double-click on the **SnappyDataODBCDriverInstaller.msi** file, and follow the steps to complete the installation.

	!!! Note
		Ensure that [TIBCO ComputeDB is installed](../install.md) and the [TIBCO ComputeDB cluster is running](start_snappy_cluster.md).

## Connecting to the TIBCO ComputeDB Cluster 
Once you have installed the TIBCO ComputeDB ODBC Driver, you can connect to TIBCO ComputeDB cluster in any of the following ways:

* Use the TIBCO ComputeDB Driver Connection URL:

		Driver=TIBCO ComputeDB ODBC Driver;server=<ServerIP>;port=<ServerPort>;user=<userName>;password=<password> 
	        
* Create a TIBCO ComputeDB DSN (Data Source Name) using the installed TIBCO ComputeDB ODBC Driver. Refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>
When prompted, select the TIBCO ComputeDB ODBC Driver from the list of drivers and enter a Data Source name, TIBCO ComputeDB Server Host, Port, User Name and Password.
Refer to the documentation for detailed information on [Setting Up TIBCO ComputeDB ODBC Driver](../setting_up_odbc_driver-tableau_desktop.md).  

## Connecting Spotfire® Desktop to TIBCO ComputeDB
Refer [TIBCO Spotfire® Connectivity to TIBCO ComputeDB™](https://community.tibco.com/wiki/tibco-spotfire-connectivity-tibco-computedb) for detailed instructions to access TIBCO ComputeDB using this connector.

Also see:

[ODBC Supported APIs in TIBCO ComputeDB Driver](/reference/API_Reference/odbc_supported_apis.md)
