<a id="howto-odbc"></a>
# How to Connect using ODBC Driver


You can connect to SnappyData Cluster using SnappyData ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.

<a id="howto-odbc-step1"></a>
## Step 1: Installing Visual C++ Redistributable for Visual Studio 2013

To download and install the Visual C++ Redistributable for Visual Studio 2013:

1. [Download Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784)

2. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
## Step 2: Installing SnappyData ODBC Driver

To download and install the ODBC driver:

1. Download the drivers zip file **snappydata-odbc_1.3.0_win.zip** using the steps provided [here](/quickstart/getting_started_by_installing_snappydata_on-premise.md).
2. Extract **snappydata-odbc_1.3.0_win.zip**. Depending on your Windows installation, extract the contents of the 32-bit or 64-bit version of the SnappyData ODBC Driver.

    | Version | ODBC Driver |
    |---------|-------------|
    |32-bit for 32-bit platform|snappydata-odbc_1.3.0_win_x86.msi|
    |64-bit for 64-bit platform|snappydata-odbc_1.3.0_win_x64.msi|

4. Double-click on the corresponding **msi** file, and follow the steps to complete the installation.

	!!! Note
		Ensure that [SnappyData is installed](../install.md) and the [SnappyData cluster is running](start_snappy_cluster.md).

## Connecting to the SnappyData Cluster
Once you have installed the SnappyData ODBC Driver, you can connect to SnappyData cluster in any of the following ways:

* Use the SnappyData Driver Connection URL:

		Driver=SnappyData ODBC Driver;server=<ServerIP>;port=<ServerPort>;user=<userName>;password=<password>

* Create a SnappyData DSN (Data Source Name) using the installed SnappyData ODBC Driver. Refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>
When prompted, select the SnappyData ODBC Driver from the list of drivers and enter a Data Source name, SnappyData Server Host, Port, User Name and Password.
Refer to the documentation for detailed information on [Setting Up SnappyData ODBC Driver](../setting_up_odbc_driver-tableau_desktop.md).

## Connecting Spotfire® Desktop to SnappyData
Refer [TIBCO Spotfire® Connectivity to SnappyData™](https://community.tibco.com/wiki/tibco-spotfire-connectivity-tibco-computedb) for detailed instructions to access SnappyData using this connector.

Also see:

[ODBC Supported APIs in SnappyData Driver](/reference/API_Reference/odbc_supported_apis.md)
