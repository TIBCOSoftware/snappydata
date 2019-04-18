# How to Access TIBCO ComputeDB™ from Various SQL Client Tools

You can use any of the following SQL client tools to access TIBCO ComputeDB:

*	[DbVisualizer](#dbvis)
*	[SQL Workbench/J](#sqlworkbenchj)

<a id= dbvis> </a>
## DbVisualizer
DbVisualizer is a universal database tool that can be used to access databases. To access TIBCO ComputeDB from DbVisualizer, you must create a database connection using a database driver.

To access TIBCO ComputeDB from DbVisualizer, do the following:

### Create a Database Driver

1.	On the DbVisualizer home page, go to **Tools** > **Driver Manager**  and click **+**.
2.	Enter the following details:
	*	Name: Provide a name for the driver.
    *	URL Format: Specify the URL format as *jdbc:snappydata://server:port/*
	*	Driver Class: Choose the driver class as **io.snappydata.jdbc.ClientDriver**. From the **Driver jar Files**, click the folder icon and select the client jar. Only after you select the client jar, the **io.snappydata.jdbc.ClientDriver** class is listed as an option in Driver Class. For TIBCO ComputeDB, this jar is located in the **build-artifacts/scala-2.11/distributions/** folder.

### Create a Database Connection

1. On the main dbviz UI, click the **Create new database** icon.<br>You are given the options to either use or not use a wizard for the connection. 
2. Click **Use Wizard**. 
3. In the **New Connection** dialog box, enter a name that can be used to refer to the database connection.
4. In the **Select Database Driver** dialog box, select the database driver that you have created.
5. In the **Database URL** dialog box, type the complete URL. For example: *jdbc:computedb://localhost:1527/*.
6. Enter the UserID and password.
7. Select Auto Commit if you want to enable auto-commit in the SQL Commander.
8. Click **Finish**.<br>You can open the SQL Commander and run your queries. 

!!! Note
	The steps provided here are specific to DbVisualizer 10.0.20 version. The steps can slightly vary in other versions.

<a id= sqlworkbenchj> </a>
## SQL Workbench/J

SQL Workbench/J is a free, DBMS-independent, cross-platform SQL query tool which can be run on any operating system that provides a Java Runtime Environment. It can also work with a JDK (Java Development Kit). 

### Prerequisites

*	Install Java 8 or higher to run SQL Workbench/J. Using a 64-bit Java runtime is highly recommended.
*	Download and install SQL Workbench/J.

###Connecting to TIBCO ComputeDB from SQL Workbench/J

1.	Start the snappy cluster and Snappy shell in local mode. 
2.	Start SQL Workbench/J.  <br> If you are using Microsoft Windows® start the application, double-click the **SQLWorkbench.exe** executable to start. If you are using a 64-bit operating system and a 64-bit Java runtime, you have to use **SQLWorkbench64.exe** instead. <br>If you are running Linux or another Unix® like operating system, you can use the supplied shell script **sqlworkbench.sh** to start the application.<br> The **Select Connection Profile** dialog box is displayed.
3.	In the **Default Group** section, enter a new name for the new profile and click the **Save** icon. 
4.	Click **Manage Drivers** from the bottom left. The **Manage driver** dialog box is displayed. 
5.	Enter the following details:
	*	**Name**: Provide a name for the driver. 
	*	**Library**: Click the folder icon and select the JDBC Client jar. <br> You must download the JDBC Client jar (snappydata-jdbc_2.11-1.0.2.2.jar) from the TIBCO ComputeDB website to your local machine.
	*	**Classname**: **io.snappydata.jdbc.ClientDriver**. 
	*	**Sample** **URL**: jdbc:snappydata://<server>:<port>/
6.	Click **OK**. The **Select Connection Profile** page is displayed.
7.	Do the following:
	* Select the driver that was created.
	* Provide the URL as *jdbc:snappydata://localhost:1527/*
	* Enter username and password.
8. Click the **Test** button and then click **OK**. <br>If the connection is successful, a message is displayed. You can now run queries in TIBCO ComputeDB from SQL WorkBench/J.








