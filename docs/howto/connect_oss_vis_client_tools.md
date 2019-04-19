# How to Access TIBCO ComputeDB™ from Various SQL Client Tools

You can use any of the following SQL client tools to access TIBCO ComputeDB:

*	[DbVisualizer](#dbvis)
*	[SQL Workbench/J](#sqlworkbenchj)
*	[DBeaver](#dbeaver)
*	[Squirrel](#squirrel)

<a id= dbvis> </a>
## DbVisualizer
DbVisualizer is a universal database tool that can be used to access databases. You can access TIBCO ComputeDB from DbVisualizer when you create a database connection using a database driver.

To access TIBCO ComputeDB from DbVisualizer, do the following:

### Create a Database Driver

To create a database driver, do the following:

1.	On the DbVisualizer home page, go to **Tools** > **Driver Manager** and click **+**.
2.	Enter the following details:

	*	Name: Provide a name for the driver.
    *	URL Format: Specify the URL format as *jdbc:snappydata://server:port/*
	*	Driver Class: Choose the driver class as **io.snappydata.jdbc.ClientDriver**. From the **Driver jar Files**, click the folder icon and select the client jar. Only after you select the client jar, the **io.snappydata.jdbc.ClientDriver** class is listed as an option in Driver Class.<br> For TIBCO ComputeDB, this jar is located in the **build-artifacts/scala-2.11/distributions/** folder.

### Connecting to TIBCO ComputeDB from DbVisualizer

To connect TIBCO ComputeDB from DbVisualizer, do the following:

1. On the main dbviz UI, click the **Create new database** icon.<br>You are given the options to either use or not use a wizard for the connection. 
2. Click **Use Wizard**. 
3. In the **New Connection** dialog box, enter a name that can be used to refer to the database connection.
4. In the **Select Database Driver** dialog box, select the database driver that you have created.
5. In the **Database URL** dialog box, type the complete URL. For example: *jdbc:snappydata://localhost:1527/*.
6. Enter the UserID and password.
7. Select the **Auto Commit** option if you want to enable auto-commit in the SQL Commander.
8. Click **Finish**.<br>You can open the SQL Commander and run your queries.

!!! Note
	The steps provided here are specific to DbVisualizer 10.0.20 version. The steps can slightly vary in other versions.

<a id= sqlworkbenchj> </a>
## SQL Workbench/J

SQL Workbench/J is a free, DBMS-independent, cross-platform SQL query tool which can be run on any operating system that provides a Java Runtime Environment. It can also work with a JDK (Java Development Kit). 

### Prerequisites

*	Install Java 8 or higher to run SQL Workbench/J. Using a 64-bit Java runtime is highly recommended.
*	Download and install SQL Workbench/J.

### Connecting to TIBCO ComputeDB from SQL Workbench/J

To connect TIBCO ComputeDB from SQL Workbench/J, do the following:

1.	Start the TIBCO ComputeDB cluster and Snappy shell in local mode. 
2.	Start SQL Workbench/J.  <br> If you are using Microsoft Windows®, double-click the **SQLWorkbench.exe** executable to start. If you are using a 64-bit operating system and a 64-bit Java runtime, you have to use **SQLWorkbench64.exe** instead. <br>If you are running Linux or another Unix® like operating system, you can use the supplied shell script **sqlworkbench.sh** to start the application.<br> The **Select Connection Profile** dialog box is displayed.
3.	In the **Default Group** section, enter a name for the new profile and click the **Save** icon. 
4.	Click **Manage Drivers** from the bottom left. The **Manage driver** dialog box is displayed. 
5.	Enter the following details:
	*	**Name**: Provide a name for the driver. 
	*	**Library**: Click the folder icon and select the JDBC Client jar. <br> You must download the JDBC Client jar (snappydata-jdbc_2.11-1.0.2.2.jar) from the TIBCO ComputeDB website to your local machine.
	*	**Classname**: **io.snappydata.jdbc.ClientDriver**. 
	*	**Sample** **URL**: jdbc:snappydata://server:port/
6.	Click **OK**. The **Select Connection Profile** page is displayed.
7.	Do the following:
	* Select the driver that was created.
	* Provide the URL as *jdbc:snappydata://localhost:1527/*
	* Enter username and password.
8. Click the **Test** button and then click **OK**. <br> After you get a successful connection, you run queries in TIBCO ComputeDB from SQL WorkBench/J.

<a id= dbeaver> </a>
## DBeaver
Before you connect TIBCO ComputeDB from DBeaver, you must start the LDAP server.

### Starting the LDAP Server

To start the LDAP server, do the following:

1.	From the terminal, go to the location of ldap-test-server: <br> `cd $SNAPPY_HOME/store/ldap-test-server`
2.	Run the following command to build: <br>`./gradlew build`
3.	Run the script: <br>`./start-ldap-server.sh auth.ldif`<br>
	This starts the LDAP server and prints the LDAP conf. The printed LDAP conf contains the username and password of LDAP that should be used to connect from DBeaver. Copy this into all the conf files of TIBCO ComputeDB.
4.	Start the TIBCO ComputeDB cluster.

### Connecting to TIBCO ComputeDB from DBeaver

To connect TIBCO ComputeDB from DBeaver, do the following:

1.	Go to the [Download page](https://dbeaver.io/download/) of DBeaver.
2.	Choose an appropriate installer for the corresponding operating system. For example, for Linux Debian package, download from [this link](https://dbeaver.io/files/dbeaver-ce_latest_amd64.deb).
3.	Run the corresponding commands that are specified in the **Install** section on the Download page.
4.	Launch DBeaver and click **New database connection**. 
5.	Select **Hadoop / Big Data** section from the left. 
6.	Select TIBCO ComputeDB from the available list of databases and provide the following details:
	*	Hostname/IP
	*	Port
	*	Username / Password
7.	Test the connection and finish the setup of the database source.

<a id= squirrel> </a>
## Squirrel

Before you connect TIBCO ComputeDB from Squirrel, you must start the LDAP server.

### Starting the LDAP Server

To start the LDAP server, do the following:

1.	From the terminal, go to the location of ldap-test-server: <br> `cd $SNAPPY_HOME/store/ldap-test-server`
2.	Run the following command to build: <br>`./gradlew build`
3.	Run the script: <br>`./start-ldap-server.sh auth.ldif`
	This starts the LDAP server and prints the LDAP conf. The printed LDAP conf contains username and password of LDAP that should be used to connect from Squirrel. Copy this into all the conf files of TIBCO ComputeDB.
4.	Start TIBCO ComputeDB cluster.

### Download and Install Squirrel

To download and install Squirrel, do the following:

1.	[Download](https://sourceforge.net/projects/squirrel-sql/) Squirrel.
2.	Go to the folder where Squirrel jar is downloaded and run the following command to install Squirrel:<br>
	`java -jar <downloaded squirrel jar>`
3.	Go to the Squirrel installation folder and run the following command:<br> 
	`./squirrel-sql.sh`

### Connecting to TIBCO ComputeDB from Squirrel	

To connect TIBCO ComputeDB from Squirrel, do the following:

1.	In the **Drivers** tab on the left side, click **+** sign to add a new driver. 
2.	Provide the following details:
	*	Name
	*	Example URL(connection string)
	*	website URL
3.	Add the downloaded **snappydata jdbc jar** in the extra classpath tab and provide the class name to be used for the connection. <br>
	```
jdbc jar: https://mvnrepository.com/artifact/io.snappydata/snappydata-jdbc_2.11/1.0.2.2
	jdbc class: io.snappydata.jdbc.ClientPoolDriver
```
4.	Go to **Aliases** tab and then click **+** to add a new alias. 
5.	Provide the following details: 
	*	Name
	*	Driver (added in step 4) 
	*	JDBC URL (For example: jdbc:snappydata:pool://localhost:1527)
	*	Username and password: (Default username / password: app/app)
6.	Test the connection and finish setup.
	You can run the following test queries:

            create table colTable(CustKey Integer not null, CustName String) using column options();
            insert into colTable values(1, 'a');
            insert into colTable values(2, 'a');
            insert into colTable values(3, 'a');
            select count(*) from colTable;
            create table rowTable(CustKey Integer NOT NULL PRIMARY KEY,  CustName String) using row options();
            insert into rowTable values(11, 'a');
            insert into rowTable values(22, 'a');
            insert into rowTable values(33, 'a');
            update rowTable set CustName='d' where custkey = 1;
            select * from rowTable order by custkey;
            drop table if exists rowTable;
            drop table if exists colTable;
            show tables;




