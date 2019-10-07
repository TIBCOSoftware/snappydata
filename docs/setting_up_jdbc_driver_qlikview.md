# Setting Up SnappyData JDBC Client and QlikView

!!! Note
	Before using SnappyData JDBC Client, make sure [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) is installed.

The following topics are covered in this section:

* [Step 1: Download SnappyData JDBC Client](#step-1)

* [Step 2: Download and Install QlikView](#step-2)

* [Step 3: Download and Install JDBCConnector for QlikView](#step-3)

* [Step 4: Configure the JDBCConnector to connect to SnappyData](#step-4)

* [Step 5: Connecting from QlikView to SnappyData](#step-5)

<a id= step-1> </a>
## Step 1: Download SnappyData JDBC Client

*	[Download the SnappyData JDBC Client JAR](https://github.com/SnappyDataInc/snappydata/releases/latest).
	</br>See also: [How to connect using JDBC driver](/howto/connect_using_jdbc_driver.md)

<a id= step-2> </a>
## Step 2: Download and Install QlikView
1. [Download a QlikView installation package](https://www.qlik.com/us/download).

2.  Double-click the **Setup.exe** file to start the installation. For installation instructions refer to the QlikView  documentation.

<a id= step-3> </a>
## Step 3: Download and Install JDBCConnector for QlikView

To connect to SnappyData using JDBC Client from QlikView application, install the JDBCConnector. This connector integrates into the QlikView application

1. [Download JDBCConnector installer](https://www.tiq-solutions.de/en/products/qlikview/jdbc-connector/).

2. Extract the contents of the compressed file, and double-clik on the installer to start the installation process. For installation instructions, refer to the documentation provided for the QlikView JDBC Connector. </br>You may need to activate the product.

<a id= step-4> </a>
## Step 4: Configure the JDBCConnector to Connect to SnappyData

After installing the JDBCConnector application, add the SnappyData profile in the JDBCConnector. 

!!! Tip
	You can also create a profile from the QlikView application.

1. Open JDBCConnector Application.

2. In the **Profiles** tab, click **Create Profile**.

3. Enter a profile name. For example, SnappyData. 

4. Click **Set As Default** to set it as the default profile.

5. In the **Java VM Options** tab, click **Select JVM**, to set the path for the **jvm.dll** file. <br> For example, C:\Program Files\Java\jre1.8.0_121\bi\server\jvm.dll.

6. Click **Add**, to add/update option **-Xmx1024M**.

7. In the **JDBC Driver** tab, select the path to the **snappydata-client-1.6.0.jar** file.

8. In the **Advanced** tab, add the JDBC Driver Classname **io.snappydata.jdbc.ClientDriver**.

9. Click **OK** to save and apply your changes.

<a id= step-5> </a>
## Step 5: Connecting from QlikView to SnappyData

1. Open the QlikView desktop application.

2. Click **File > New** from the menu bar to create a QlikView workbook.</br> The Getting Started Wizard is displayed. Close it to continue.

3. Click **File > Edit Script** from the menu bar.

4. In the **Data** tab, select **JDBCConnector_x64.dll** from the **Database** drop down.

5. Click **Configure**. Verfiy that the following configuration is displayed:
	* In the Java VM Options tab, the path to **jvm.dll** file is correct and also the add/update option displays **-Xmx1024M**.
	* In the JDBC Driver tab, the path to the **snappydata-client-1.6.0.jar** file is correct.
	* In the Advanced tab, JDBC Driver class name is displayed as **io.snappydata.jdbc.ClientDriver**.

6. Click **Connect**. The Connect through QlikView JDBC Connector window is displayed.

	* In **URL** field enter the SnappyData JDBC URL in the format **jdbc:snappydata://<host>:<port>** </br> For example, jdbc:snappydata://192.168.1.200:1527. 

	* Enter both the Username and Password as **app**.

7. Click **OK**to apply the changes.

8. When the connection is successful, **CONNECT TO** script is added to the list of scripts in **Edit Script** panel.

9. Click the **Select** button to add the Data Source Table (SELECT sql script) OR you can manually add the *SELECT* sql script.

10. The ***SELECT*** script is added to the list of scripts.

11. Click **OK**.

12. After adding the desired Data Source Table, click **OK** on the **Edit Script** panel.

13. From the menu bar, click **File > Reload** to load data from the data source.

14. From the menu bar, click **Tools > Quick Chart Wizard** to add the required charts.

15. The **Selected charts** wizard guides you for generating data visualizations. </br>Refer to the QlikView documentation for more information on data visualization.

