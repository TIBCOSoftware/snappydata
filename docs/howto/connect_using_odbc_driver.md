<a id="howto-odbc"></a>
# How to Connect using ODBC Driver

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

You can connect to TIBCO ComputeDB Cluster using TIBCO ComputeDB ODBC Driver and can execute SQL queries by connecting to any of the servers in the cluster.

<a id="howto-odbc-step1"></a>
## Step 1: Installing Visual C++ Redistributable for Visual Studio 2013

To download and install the Visual C++ Redistributable for Visual Studio 2013:

1. [Download Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784)

2. Select **Run** to start the installation and follow the steps to complete the installation.

<a id="howto-odbc-step2"></a>
## Step 2: Installing TIBCO ComputeDB ODBC Driver

To download and install the ODBC driver:

1. [Download the TIBCO ComputeDB 1.1.1 Enterprise Version](https://edelivery.tibco.com/storefront/index.ep). The downloaded file contains the TIBCO ComputeDB ODBC driver installers.

2. Depending on your Windows installation, extract the contents of the 32-bit or 64-bit version of the TIBCO ComputeDB ODBC Driver.

    | Version | ODBC Driver |
    |--------|--------|
    |32-bit for 32-bit platform|TIB_compute-odbc_1.1.1_win_x86_32bit.zip|
    |64-bit for 64-bit platform|TIB_compute-odbc_1.1.1_win_x64_64bit.zip|

4. Double-click on the extracted **TIB_compute-odbc_1.1.1_win.msi** file, and follow the steps to complete the installation.

	!!! Note
		Ensure that [TIBCO ComputeDB is installed](../install.md) and the [TIBCO ComputeDB cluster is running](start_snappy_cluster.md).

## Connecting to the TIBCO ComputeDB Cluster 
Once you have installed the TIBCO ComputeDB ODBC Driver, you can connect to TIBCO ComputeDB cluster in any of the following ways:

*	Use the TIBCO ComputeDB Driver Connection URL:

			Driver=TIBCO ComputeDB ODBC Driver;server=<locator address>;port=<LocatorPort>;user=<userName>;password=<password>
	
    In case you want to connect with a specific server:
    
    		Driver=TIBCO ComputeDB ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>;load-balance=false

*	Create a TIBCO ComputeDB DSN (Data Source Name) using the installed TIBCO ComputeDB ODBC Driver. Refer to the Windows documentation relevant to your operating system for more information on creating a DSN. </br>
When prompted, select the TIBCO ComputeDB ODBC Driver from the list of drivers and enter a Data Source name. You can then enter either TIBCO ComputeDB Server Host, Port, User Name, and Password or TIBCO ComputeDB Locator Host, Port, User Name and Password.
Refer to the documentation for detailed information on [Setting Up TIBCO ComputeDB ODBC Driver](../setting_up_odbc_driver-tableau_desktop.md).  

## Connecting Spotfire® Desktop to TIBCO ComputeDB
Refer [TIBCO Spotfire® Connectivity to TIBCO ComputeDB™](https://community.tibco.com/wiki/tibco-spotfire-connectivity-tibco-computedb) for detailed instructions to access TIBCO ComputeDB using this connector.

Also see:

[ODBC Supported APIs in TIBCO ComputeDB Driver](/reference/API_Reference/odbc_supported_apis.md)
