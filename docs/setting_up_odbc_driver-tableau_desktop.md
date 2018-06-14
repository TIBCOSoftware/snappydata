# Setting Up SnappyData ODBC Driver and Tableau Desktop

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

!!! Note: 
	* This is currently tested and supported only on Windows 10 (32-bit and 64-bit systems).

    * [Download and Install Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784) 

## Step 1: Install the SnappyData ODBC Driver

1. [Download the SnappyData 1.0.1 Enterprise Edition](http://snappydatainc.github.io/snappydata/install/#download-snappydata).

2. Click **ODBC INSTALLERS** to download the **snappydata-odbc-1.0.0.zip** file.

3. Follow [steps 1 and 2](http://snappydatainc.github.io/snappydata/howto/connect_using_odbc_driver) to install the  SnappyData ODBC driver.

## Step 2: Create SnappyData DSN from ODBC Data Sources 64-bit/32-bit

To create SnappyData DSN from ODBC Data Sources:

1. Open the **ODBC Data Source Administrator** window:

	a. On the **Start** page, type ODBC Data Sources, and select **Set up ODBC data sources** from the list or select **ODBC Data Sources** in the **Administrative Tools**.

	b. Based on your Windows installation, open **ODBC Data Sources (64-bit)** or **ODBC Data Sources (32-bit)**

2. In the **ODBC Data Source Administrator** window, select either the **User DSN** or **System DSN** tab. 

3. Click **Add** to view the list of installed ODBC drivers on your machine.

4. From the list of drivers, select **SnappyData ODBC Driver** and click **Finish**.

5. The **SnappyData ODBC Configuration** dialog is displayed. </br>Enter the following details to create a DSN:

	* **Data Source Name**: Name of the Data Source. For example, *snappydsn*.  

	* **Server (Hostname or IP)**: IP address of the data server which is running in the SnappyData cluster.

	* **Port**: Port number of the server. By default, it is **1528** for the first data server in the cluster.

	* **Login ID**: The login ID required to connect to the server. For example, *app*

	* **Password**: The password required to connect to the server. For example, *app*

!!! Note: 
	Ensure that you provide the IP Address/Host Name and Port number of the data server. If you provide the details of the locator, the connection fails. 

## Step 3. Install Tableau Desktop (10.1 or Higher)

To install Tableau desktop:

1. [Download Tableau Desktop](https://www.tableau.com/products/desktop).

2. Depending on your Windows installation, download the 32-bit or 64-bit version of the installer.

3. Follow the steps to complete the installation and ensure that you register and activate your product.

## Step 4. Connecting Tableau Desktop to SnappyData Server

When using Tableau with the SnappyData ODBC Driver for the first time, you must add the **odbc-snappydata.tdc** file, that is available in the downloaded **snappydata-odbc-1.0.0.zip**.

To connect the Tableau Desktop to the SnappyData Server:

1. Copy the **odbc-snappydata.tdc** file to the <_User_Home_Path_>/Documents/My Tableau Repository/Datasources directory.

2. Open the Tableau Desktop application.

3. On the Start Page,

	a. Under **Connect** > **To a Server**, click **Other Databases (ODBC)**. The Other Databases (ODBC) window is displayed.

	b. In the DSN drop-down list, select the name that you provided for your SnappyData ODBC connection (for example *snappydsn*), and then click **Connect**.

4. When connection to the SnappyData server is established, the **Sign In** option is enabled. Click **Sign In** to log into Tableau.

5. From the **Schema** drop-down list, select a schema. For example, *app*. </br>All tables from the selected schema are listed.

6. Select the required table(s) and drag it to the canvas. A view generated using the selected tables is displayed. </br>If you make changes to the table, click **Update Now** to refresh and view your changes.

7. In the **Worksheets** tab, click **sheet** to start the analysis.</br> 

8. On this screen, you can click and drag a field from the **Dimensions** area to **Rows** or **Columns**.</br> Refer to the Tableau documentation for more information on data visualization.


