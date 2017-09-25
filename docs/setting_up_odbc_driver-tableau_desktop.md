# Setting Up SnappyData ODBC Driver and Tableau Desktop

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

!!! Note: 
	* This is currently tested and supported only on Windows 10 (32-Bit & 64-Bit systems).

    * [Download and Install Visual C++ Redistributable for Visual Studio 2013](https://www.microsoft.com/en-in/download/details.aspx?id=40784) 

## Step 1: Install the SnappyData ODBC Driver
[Download the SnappyData 1.0-RC Enterprise Version](http://www.snappydata.io/download) by registering on the SnappyData website and [install the SnappyData ODBC Driver](howto/connect_using_odbc_driver.md#howto-odbc-step2).

## Step 2: Create SnappyData DSN from ODBC Data Sources 64-bit/32-bit

To create SnappyData DSN from ODBC Data Sources:

1. Open the **ODBC Data Source Administrator** window:

	a. On the **Start** page, type ODBC Data Sources, and select **Set up ODBC data sources** from the list or select **ODBC Data Sources** in the **Administrative Tools**.

	b. Based on your Windows installation, open **ODBC Data Sources (64-bit)** or **ODBC Data Sources (32-bit)**

2. In the **ODBC Data Source Administrator** window, select either the **User DSN** or **System DSN** tab. 

3. Click **Add** to view the list of installed ODBC Drivers on your machine.

4. From the list of drivers, select **SnappyData ODBC Driver** and click **Finish**.

5. The **SnappyData ODBC Configuration** dialog is displayed. Enter the following details to create DSN:

	* **Data Source Name**: Name of the Data Source. For example, snappydsn.  

	* **Server (Hostname or IP)**: IP address of the data server which is running in the SnappyData cluster.

	* **Port**: Port number of the server. By default, it is **1528** for the first data server in the cluster.

	* **Login ID**: The login ID required to connect to the server. For example, **app**

	* **Password**: The password required to connect to the server. For example, **app**

!!! Note: 
	Ensure that you provide the IP Address/Host Name and Port number of the data server. If you provide the details of the locator, the connection fails. 

## Step 3. Install Tableau Desktop (10.1 or Higher)

To install Tableau desktop:

1. [Download Tableau Desktop](https://www.tableau.com/products/desktop).

2. Depending on your Windows installation, download the 32-bit or 64-bit version of the installer. 

3. Follow the steps to complete the installation.

## Step 4. Connecting Tableau Desktop to SnappyData Server

Before using Tableau with SnappyData ODBC Driver for the first time, you must add the **odbc-snappydata.tdc** configuration file in the Tableau data sources directory. Follow below steps to do so.

To connect the Tableau Desktop to SnappyData Server:

1. The downloaded SnappyData file contains the **odbc-snappydata.tdc** configuration file.

2. Copy the **odbc-snappydata.tdc** file to the <_User_Home_Path_>/Documents/My Tableau Repository/Datasources directory.

3. Open the Tableau Desktop application

4. On the start page, under **Connect** > **To a Server**, and then click, **Other Databases (ODBC)**.
The **Other Databases (ODBC)** window is displayed. 

5. In the **Connect Using > DSN** drop-down, select the data source name provided earlier. (eg. snappydsn).

6. Click **Connect**.

7. When a connection to SnappyData Server is established, the **Sign In** option is enabled. 

8. Click **Sign In** to log into Tableau.

9. From the **Schema** drop-down list, select a schema. For example, **app**

10. All tables from the selected schema is listed.

11. Select the required table(s) and drag it to the canvas. A view generated using the selected tables is displayed. </br>If you make changes to the table, click **Update Now** to refresh and view your changes.

12. Click the **Worksheets** tab > **sheet** to start the analysis.</br> 

13. On this screen, you can click and drag a field from the **Dimensions** area to** Rows** or **Columns**. Refer to the Tableau documentation for more information on data visualization.


