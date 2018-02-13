# Setting Up SnappyData JDBC Client and QlikView

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

!!! Note: 
	* This is currently tested and supported only on Windows 10 (32-bit and 64-bit systems).
	
	* Building SnappyData JDBC Client requires [JDK 8 64-bit installation (Oracle Java SE)](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

## Step 1: Download SnappyData JDBC Client

1. [Download the SnappyData JDBC Client JAR](https://www.snappydata.io/download).

2. Follow [steps 1 and 2](howto/connect_using_odbc_driver.md) to install the  SnappyData ODBC driver.

## Step 2: Download and Install QlikView

1. [Download](https://www.qlik.com/us/try-or-buy/download-qlikview).

2. Follow the installation instruction to install QlikView. 


## Step 3: Download and Install JDBCConnector for QlikView

To connect the QlikView application to SnappyData, you need to install the JDBCConnector. This connector integrates into the QlikView application.

1. [Download JDBCConnector installer](https://www.tiq-solutions.de/en/products/qlikview/jdbc-connector/).

2. Install the JDBC Connector. You may need to activate the product.


## Step 4: Configure JDBCConnector to connect to SnappyData
After installing the JDBCConnector application, add the SnappyData profile in JDBCConnector (this can also be done from QlikView application).

1. Open JDBCConnector Application.

2. In the **Profiles** tab, click **Create Profile**.

3. Enter a profile name. For example, SnappyData. 

4. Click **Set As Default** to set it as the default profile.

5. In the **Java VM Options** tab, click **Select JVM**, to set the path for the **jvm.dll** file. <.br> For example, C:\Program Files\Java\jre1.8.0_121\bi\server\jvm.dll.

6. Click Add, to add/update option **-Xmx1024M**.

7. In the **JDBC Driver** tab, select the path to the **snappydata-client-1.6.0.jar** file.

8. In the **Advanced** tab, add the JDBC Driver Classname **io.snappydata.jdbc.ClientDriver**.

9. Click **OK **to save and apply your changes.


## Step 5: Connecting from QlikView to SnappyData

1. Open the QlikView desktop application.

2. Create new workbook by clicking on **New** button from toolbar or click **File > New**.
	The Getting Started Wizard is displayed. Close it to continue.

3. Go to Menu and click on **File > Edit Script**.

4. In the **Data** tab, select **JDBCConnector_x64.dll** from the **Databases** drop down.

5. Click **Configure**. Verfiy that the following configuration is displayed:
	* In the Java VM Options tab, the path to **jvm.dll** file is correct and also the add/update option displays **-Xmx1024M**.
	* In the JDBC Driver tab, the path to the **snappydata-client-1.6.0.jar** file is correct.
	* In the Advanced tab, JDBC Driver class name is displayed as **io.snappydata.jdbc.ClientDriver**.

6. Click Connect. The Connect through QlikView JDBC Connector window is displayed.
	* In **URL** field enter the SnappyData JDBC URL in the format **jdbc:snappydata://<host>:<port>** </br> For example, jdbc:snappydata://192.168.1.200:1527. 
	* Enter both the Username and Password as **app**.

7. Click **OK **to apply the changes.

8. When the connection is successful, **CONNECT TO** script is added to the list of scripts in **Edit Script** panel.

9. Click the** Select** button to add the Data Source Table (SELECT sql script) OR you can manually add the *SELECT* sql script.

10. The *SELECT* script is added to the list of scripts.

11. Click **OK**.

12. From the toolbar, click **File** > **Reload** to load data from the data source.

13. From the toolbar, click **Tools** > **Quick Chart Wizard** to add the required charts.

14. The **Selected charts** wizard will guide you for generating data visualizations. </br>Refer to the QlikView documentation for more information on data visualization.


