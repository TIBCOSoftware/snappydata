# Setting Up SnappyData ODBC Driver and Tableau Desktop


!!! Note: 
	This is currently tested and supported only on Windows 10 (32-Bit & 64-Bit systems). Although, versions other than Windows 10 may work, SnappyData is not claiming full testing on other versions.

## Step 1 and 2:
Ensure that you do the following:
1. [Install Visual C++ Redistributable for Visual Studio 2015](howto.md/#howto-odbc-step1)
2. [Install SnappyData ODBC Driver](howto.md/#howto-odbc-step2)

## Step 3: Create SnappyData DSN from ODBC Data Sources 64-bit/32-bit

To create SnappyData DSN from ODBC Data Sources:

1. Open the **ODBC Data Source Administrator** window:

	a. On the **Start** page, type ODBC Data Sources, and select Set up ODBC data sources from the list or Select  **ODBC Data Sources** in the **Administrative Tools**. 

	b. Based on your Windows installation, open **ODBC Data Sources (64-bit)** or **ODBC Data Sources (32-bit)**

2. In the** ODBC Data Source Administrator** window, select either the **User DSN** or **System DSN** tab. 

3. Click **Add** to view the list of installed ODBC Drivers on your machine.

4. From the list of drivers, select **SnappyData ODBC Driver** and click **Finish**.

5. The **SnappyData ODBC Configuration Dialog** is displayed. Enter the following details to create DSN:
	* **Data Source Name**: Name of the Data Source. For example, snappydsn.  

	* **Server (Hostname or IP)**: IP address of the data server which is running in the SnappyData cluster.

	* **Port**: Port number of the server. By default it is **1528** for the first data server in the cluster.

	* **Login ID**: The login ID required to connect to the server. Fir example, **app**

	* **Password**: The password required to connect to the server. For example, **app**

!!! Note: 
	Ensure that you provide the IP Address/Host Name and Port number of the data server. If you provide the details of the locator, the connection fails. 

## Step 4. Install Tableau Desktop (10.1 or Higher)

To install Tableau desktop:

1. [Download Tableau Desktop](https://www.tableau.com/products/desktop).

2. Depending on your Windows installation, download the 32-bit or 64-bit version of the installer. 

3. Follow the steps to complete the installation.

## Step 5. Connecting Tableau Desktop to SnappyData Server

Before using Tableau with SnappyData ODBC Driver for the first time, you must add the **odbc-snappydata.tdc** configuration file in the Tableau data sources directory. Follow below steps to do so.

To connect the Tableau Desktop to SnappyData Server:

1. Download **odbc-snappydata.tdc** configuration file from the [SnappyData Release page](https://github.com/SnappyDataInc/snappydata/releases).

2. Copy the downloaded odbc-snappydata.tdc file to  the <_User_Home_Path_>\Documents\My Tableau Repository\Datasources directory.

3. Open the Tableau Desktop application

4. On the start page, under **Connect** > **To a Server**, and then click, **Other Databases (ODBC)**.
The **Other Databases (ODBC)** window is displayed. 

5. In the **Connect Using > DSN** drop-down, select the data source name provided earlier. (eg. snappydsn).

6. Click **Connect**.

7. When connection to the SnappyData Server is established, the **Sign In** option is enabled. 

8. Click **Sign In** to log into Tableau.

9. From the **Schema** drop-down list, select a schema.

10. All Tables from the selected schema is listed.

11. Select required table(s) and drag it to the canvas. A view generated using the selected tables is displayed. </br>If you make changes to the table, click **Update Now** to see your changes.

12. Click the **Worksheets **tab > **sheet** to start the analysis.</br> 

13. On this screen, you can click and drag a field from the **Dimensions** area to** Rows** or **Columns**. Refer to the Tableau documenation for more information on data visualization.


