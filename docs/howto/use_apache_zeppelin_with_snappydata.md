<a id="howto-zeppelin"></a>
# How to Use Apache Zeppelin with SnappyData

## Step 1: Download, Install and Configure SnappyData
1. [Download and Install SnappyData](../install/install_on_premise.md#download-snappydata) </br>
 The table below lists the version of the SnappyData Zeppelin Interpreter and Apache Zeppelin Installer for the supported SnappyData Releases.
	
    | SnappyData Zeppelin Interpreter | Apache Zeppelin Binary Package | SnappyData Release|
	|--------|--------|--------|
    |[Version 0.7.2](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/tag/v0.7.2) |[Version 0.7.2](https://zeppelin.apache.org/download.html) |[Release 1.0.0](https://github.com/SnappyDataInc/snappydata/releases/tag/v1.0.0)|
    
2. [Configure the SnappyData Cluster](../configuring_cluster/configuring_cluster.md).

3. In [lead node configuration](../configuring_cluster/configuring_cluster.md#configuring-leads) set the following properties:

	- Enable the SnappyData Zeppelin interpreter by adding `-zeppelin.interpreter.enable=true` 

    - In the classpath option, define the location where the SnappyData Interpreter is downloaded by adding `-classpath=/<download_location>/snappydata-zeppelin-<version_number>.jar`.

    - In the **conf/spark-env.sh** file, set the `SPARK_PUBLIC_DNS` property to the public DNS name of the lead node. This enables the Member Logs to be displayed correctly to users accessing the [SnappyData Pulse UI](../monitoring/monitoring.md) from outside the network.

4. [Start the SnappyData cluster](start_snappy_cluster.md)

5. Extract the contents of the Zeppelin binary package. </br> 

6. Install the SnappyData Zeppelin interpreter in Apache Zeppelin by executing the following command from Zeppelin's bin directory: </br>
	`./install-interpreter.sh --name snappydata --artifact io.snappydata:snappydata-zeppelin:<snappydata_interpreter_version_number>`. </br>
    Zeppelin interpreter allows the SnappyData interpreter to be plugged into Zeppelin using which, you can run queries.

7. Rename the **zeppelin-site.xml.template** file (located in zeppelin-<_version_number_>-bin-all/conf directory) to **zeppelin-site.xml**.

8. Edit the **zeppelin-site.xml** file, and in the `zeppelin.interpreters` property, add the following interpreter class names: `org.apache.zeppelin.interpreter.SnappyDataZeppelinInterpreter,org.apache.zeppelin.interpreter.SnappyDataSqlZeppelinInterpreter`.

9. Start the Zeppelin daemon using the command: </br> `bin/zeppelin-daemon.sh start`.

10. To ensure that the installation is successful, log into the Zeppelin UI (**http://localhost:8080**) from your web browser.

## Step 2: Configure SnappyData for Apache Zeppelin

1. Log on to Zeppelin from your web browser and select **Interpreter** from the **Settings** option.

2. Click **Create** ![Create](../Images/create_interpreter.png) to add an interpreter.	 

3. From the **Interpreter group** drop-down select **snappydata**.
	 ![Configure Interpreter](../Images/snappydata_interpreter_properties.png)

	!!! Note: 
    	If **snappydata** is not displayed in the **Interpreter group** drop-down list, try the following options, and then restart Zeppelin daemon: 

    	* Delete the **interpreter.json** file located in the **conf** directory (in the Zeppelin home directory).

    	* Delete the **zeppelin-spark_<_version_number_>.jar** file located in the **interpreter/snappydata** directory (in the Zeppelin home directory).


4. Click the **Connect to existing process** option. The fields **Host** and **Port** are displayed.

5. Specify the host on which the SnappyData lead node is executing, and the SnappyData Zeppelin Port (Default is 3768).
	
	| Property | Default Values | Description |
	|--------|--------| -------- |
	|Host|localhost        |Specify host on which the SnappyData lead node is executing  |
	|Port        |3768        |Specify the Zeppelin server port  |
	
6. Configure the interpreter properties. </br>The table lists the properties required for SnappyData.

	| Property | Value | Description |
	|--------|--------| -------- |
	|default.ur|jdbc:snappydata://localhost:1527/	| Specify the JDBC URL for SnappyData cluster in the format `jdbc:snappydata://<locator_hostname>:1527` |
	|default.driver|com.pivotal.gemfirexd.jdbc.ClientDriver| Specify the JDBC driver for SnappyData|
	|snappydata.connection|localhost:1527| Specify the `host:clientPort` combination of the locator for the JDBC connection |
	|master|local[*]| Specify the URI of the spark master (only local/split mode) |
	|zeppelin.jdbc.concurrent.use|true| Specify the Zeppelin scheduler to be used. </br>Select **True** for Fair and **False** for FIFO | 

7. If required, edit other properties, and then click **Save** to apply your changes.</br>

8. Bind the interpreter and set SnappyData as the default interpreter.</br> SnappyData Zeppelin Interpreter group consist of two interpreters. Click and drag *<_Interpreter_Name_>* to the top of the list to set it as the default interpreter. 
 	
	| Interpreter Name | Description |
	|--------|--------|
    | %snappydata.snappydata or </br> %snappydata.spark | This interpreter is used to write Scala code in the paragraph. SnappyContext is injected in this interpreter and can be accessed using variable **snc** |
    |%snappydata.sql | This interpreter is used to execute SQL queries on the SnappyData cluster. It also has features of executing approximate queries on the SnappyData cluster.|

9. Click **Save** to apply your changes.

!!! Note: 
	You can modify the default port number of the Zeppelin interpreter by setting the property:</br>
	`-zeppelin.interpreter.port=<port_number>` in [lead node configuration](../configuring_cluster/configuring_cluster.md#configuring-leads). 
