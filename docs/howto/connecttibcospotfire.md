# How to Connect TIBCO Spotfire® Desktop to SnappyData


TIBCO Spotfire® Desktop allows users to easily author and update ad-hoc analytics, applications, and dashboards.
To connect TIBCO Spotfire® Desktop to SnappyData, setup and launch the SnappyData cluster.

You can use any of the following methods to connect TIBCO Spotfire® Desktop to SnappyData:

*	[CDB Connector using Native ODBC driver](#nativespotfireconnector)
*	[CDB Connector using Simba ODBC Driver](#builtinspotfireconnector)

<a id= nativespotfireconnector> </a>
## CDB Connector using Native ODBC Driver
TIBCO recommends to use the native Spotfire Connector to connect TIBCO Spotfire® Desktop to SnappyData. This connector is based on native ODBC and is a faster connector. Refer [TIBCO Spotfire® Connectivity to SnappyData™](https://community.tibco.com/wiki/tibco-spotfire-connectivity-tibco-computedb) for detailed instructions to access SnappyData using this connector.

<a id= builtinspotfireconnector> </a>
## CDB Connector using Simba ODBC Driver (Builtin CDB Connector in Spotfire 10.4)

Users of Spotfire® 10.4 or later can use the builtin CDB connector in Spotfire® to connect TIBCO Spotfire® Desktop to SnappyData. Refer to [Accessing Data from SnappyData](https://docs.tibco.com/pub/sfire-analyst/10.4.0/doc/html/en-US/TIB_sfire-analyst_UsersGuide/connectors/computedb/computedb_accessing_data.htm) for detailed instructions to access SnappyData using this connector.

<!---
<a id= apachesparksql> </a>
### Apache Spark SQL

Users of Spotfire versions prior to 10.4 can use Apache Spark SQL to connect TIBCO Spotfire® Desktop to SnappyData. Refer the following instructions to connect:

<a id= tibcomputsetup> </a>
### Download and Launch SnappyData

Check the [system requirements](../install/system_requirements.md), [download and install](../install/index.md) SnappyData, and then [start the SnappyData cluster](./start_snappy_cluster.md). Hive Thrift server is enabled by default. If you want to securely access the Hive Thrift server using SSL encryption, you must set these additional properties in the [Lead Node Configuration](../configuring_cluster/configuring_cluster.md#lead):

*	`-hive.server2.use.SSL=true`
*	`-hive.server2.keystore.path=<keystore-file-path>`
*	`-hive.server2.keystore.password=<keystore file password>`

For more details about setting the Hive Thrift server, refer to [Spark's documentation](https://spark.apache.org/docs/2.1.3/sql-programming-guide.html#distributed-sql-engine).

### Connecting Spotfire® Desktop to SnappyData using Apache Spark SQL

Login and download TIBCO Spotfire® Desktop from the [TIBCO eDelivery website]( https://edelivery.tibco.com/storefront/eval/tibco-spotfire-desktop/prod10954.html). Follow the setup guidelines to install the product. After installation, you can use the following steps to connect Spotfire® Desktop to SnappyData.

1.	On the Spotfire® Desktop application, from the left panel, click **Connect to** > **Apache Spark SQL** > **New Connection**. <br> The **Apache Spark SQL Connection** dialog box is displayed.<br> ![images](../Images/spotfire/generaltabspotfire.png)
2.	In the **General** tab, enter the following details:

	*	**Server**: Enter the hostname/IP of the Lead node in SnappyData cluster

    	!!! Note
    		The Hive2 Thrift server uses port 10000 by default. If you have explicitly configured the port number, then you must specify the port number as: `<IP or Hostname>:<port number>`

	*	**Authentication** **Method**: Select **username and password** option.
	*	**Username/Password**: Provide a username and password. You could choose to use APP/APP for username/password if authentication was not configured in the cluster.

3.	Select the **Use secure sockets layer** check box if you want to access the Hive Thrift server securely. Else, you can clear the check box.

	!!! Note
	Ensure to configure the lead node with additional properties for securely accessing the Hive Thrift server with SSL encryption. See [Setting SnappyData](#tibcomputsetup).

4.	Click the **Advanced** tab and set the **Thrift transport mode** to **SASL**. <br> ![images](../Images/spotfire/advancetabspotfire.png)
5.	Go to **General** tab and then click the **Connect** button.
6.	From the **Database** dropdown, either choose the existing database (schema) **app** or **default** or you can choose a database (schema) that is created in the SnappyData cluster.<br> ![images](../Images/spotfire/generaltabspotfire1.png)
7.	Click **OK**. The **View In Connection (VIC)** box is displayed which lists the tables from the selected database.
--->
