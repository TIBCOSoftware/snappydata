# How to Connect TIBCO Spotfire® Desktop to TIBCO ComputeDB™

TIBCO Spotfire® Desktop allows users to easily author and update ad-hoc analytics, applications, and dashboards. 
To connect TIBCO Spotfire® Desktop to TIBCO ComputeDB, setup and launch the TIBCO ComputeDB cluster. 

<a id= tibcomputsetup> </a>
## Setting TIBCO ComputeDB
Do the following, in the given order, to set up TIBCO ComputeDB and to enable the Hive Thrift server.

1.	Check the [system requirements](/install/system_requirements.md) to install and run TIBCO ComputeDB. 
2.	Download the Enterprise Edition from the Release page.
3.	[Configure the TIBCO ComputeDB Cluster](/configuring_cluster/configuring_cluster.md).
4.	In the [Lead Node Configuration](/configuring_cluster/configuring_cluster.md#lead), set the following property to enable the Hive Thrift server:<br>`-snappydata.hiveServer.enabled=true`.
5.	If you want to securely access the Hive Thrift server using SSL encryption, you must set these additional properties in the [Lead Node Configuration](/configuring_cluster/configuring_cluster.md#lead):
	*	`-hive.server2.use.SSL=true` 
    *	`-hive.server2.k-hive.server2.use.SSL=trueeystore.path=<keystore-file-path>` 
    *	`-hive.server2.keystore.password=<keystore file password>`
    
    For more details about setting the Hive Thrift server, refer to [placeholder].
    
6.	Launch the cluster <br> `./sbin/snappy-start-all.sh`

## Connecting Spotfire® Desktop to TIBCO ComputeDB

Login and download TIBCO Spotfire® Desktop from the [TIBCO eDelivery website]( https://edelivery.tibco.com/storefront/eval/tibco-spotfire-desktop/prod10954.html). Follow the setup guidelines to install the product. After installation, you can use the following steps to connect Spotfire® Desktop to TIBCO ComputeDB.

1.	On the Spotfire® Desktop application, from the left panel, click **Connect to** > **Apache Spark SQL** > **New Connection**. <br> The **Apache Spark SQL Connection** dialog box is displayed.<br> ![images](../Images/spotfire/generaltabspotfire.png)
3.	In the **General** tab, enter the following details:
	*	**Server**: Enter the hostname/IP of the Lead node in TIBCO ComputeDB cluster
	*	**Authentication** **Method**: Select **username and password** option.
	*	**Username/Password**: Provide a username and password. You could choose to use APP/APP for username/password if authentication was not configured in the cluster.
4.	Select the **Use secure sockets layer** check box if you want to securely access the Hive Thrift server. Else, you can clear the check box.

	!!! Note
    	Ensure to configure the lead node with additional properties for securely accessing the Hive Thrift server with SSL encryption. See [Setting TIBCO ComputeDB](#tibcomputsetup). 
        
5.	Click the **Advanced** tab and set the **Thrift transport mode** to **SASL**. <br> ![images](../Images/spotfire/advancetabspotfire.png)
6.	Go to **General** tab and then click the **Connect** button.
7.	From the **Database** dropdown, either choose the existing database (schema) **app** or **default** or you can choose a database (schema) that is created in the TIBCO ComputeDB cluster.<br> ![images](../Images/spotfire/generaltabspotfire1.png)  
8.	Click **OK**. The **View In Connection (VIC)** box is displayed which lists the tables from the selected database.



