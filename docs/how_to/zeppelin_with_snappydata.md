<a id="howto-zeppelin"></a>
##How to Use Apache Zeppelin with SnappyData
###Prerequisites
* Download the latest SnappyData source or binary from the [Zeppelin release page](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/), which lists the latest and previous releases of SnappyData. The packages are available in compressed files (.zip and .tar format). </br>
The table below lists the version of the Apache Zeppelin Interpreter and Apache Zeppelin Installer for the supported SnappyData Release.
	
    | Apache Zeppelin Interpreter | Apache Zeppelin Installer | SnappyData Release|
	|--------|--------|--------|
	|[Version 0.61](https://github.com/SnappyDataInc/snappydata/releases/download/v0.6.1/snappydata-zeppelin-0.6.1.jar)|[Version 0.6](https://zeppelin.apache.org/download.html) |[Release 0.7](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.7) and [Release 0.8](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.8) |
    |[Version 0.7](https://github.com/SnappyDataInc/zeppelin-interpreter/releases/download/v0.7.0/snappydata-zeppelin-0.7.0.jar) |[0.7](https://zeppelin.apache.org/download.html) |[Release 0.8](https://github.com/SnappyDataInc/snappydata/releases/tag/v0.8) |

* Before you begin, ensure that Zeppelin is installed. For more information on installation, refer to the Zeppelin documentation.

* All systems in the cluster, including the system on which Apache Zeppelin is installed, must be accessible at all times.

* The user should have a moderate level of knowledge in using Apache Zeppelin.

---

### Step 1: Download, Install and Configure SnappyData
1. [Download and Install SnappyData](../install/on_premise.md)

2. [Configure the SnappyData Cluster](../configuring_cluster/configuration_files.md)

3. Copy the SnappyData Zeppelin interpreter (**snappydata-<_version_number_>-bin.jar**) file to the **jars** (snappydata-<_version_number_>-bin/jars/) directory in the SnappyData home directory.

4. Enable the SnappyData Zeppelin interpreter by setting `zeppelin.interpreter.enable` to `true` in [lead node configuration](../configuring_cluster/configuration_files#configuring-leads).

5. [Start the SnappyData cluster](../how_to/start_snappydata_cluster.md)

6. Install the SnappyData interpreter in Apache Zeppelin, by executing the following command from Zeppelin's bin directory </br>
	`./install-interpreter.sh --name snappydata --artifact io.snappydata:snappydata-zeppelin:<_latest_version_>`. </br>
    Zeppelin interpreter allows the SnappyData interpreter to be plugged into Zeppelin, using which, you can run queries.

7. Rename **zeppelin-site.xml.template** to **zeppelin-site.xml**.

8. Edit the **zeppeline-site.xml** file, and add the following interpreter class names in the `zeppelin.interpreters` property: </br> `org.apache.zeppelin.interpreter.SnappyDataZeppelinInterpreter` and `org.apache.zeppelin.interpreter.SnappyDataSqlZeppelinInterpreter`.

9. Restart the Zeppelin daemon using the command </br> `bin/zeppelin-daemon.sh start`.

10. To ensure that the installation is successful, log into the Zeppelin UI (**http://localhost:8080**) from your web browser.

### Step 2: Configure SnappyData for Apache Zeppelin

1. Log on to Zeppelin from your web browser and select **Interpretor** from the **Settings** option.

2. In the **Search Interpretor** box, enter **SnappyData** and click **Search**.</br> SnappyData is displayed in the search results.

3. Configure Zeppelin to connect to the remote interpretor. </br> Select the **Connect to existing process** option, to configure Zeppelin to connect to the remote interpretor.

4. Specify the host on which the SnappyData lead node is executing, along with the SnappyData Zeppelin Port (Default is 3768).
	
	| Property | Value | Description |
	|--------|--------| -------- |
	|Host|localhost        |Specify host on which the SnappyData lead node is executing  |
	|Port        |3768        |Specify the Zeppelin server port  |
	
5. Configure the interpreter properties. </br> Click **Edit** to configure the settings required for the SnappyData interpreter. You can edit other properties if required, and then click **Save** to apply your changes.</br>	The table lists the properties required for SnappyData.

	| Property | Value | Description |
	|--------|--------| -------- |
	|default.ur|jdbc:snappydata://localhost:1527/	| Specify the JDBC URL for SnappyData cluster in the format `jdbc:snappydata://<locator_hostname>:1527` |
	|default.driver|com.pivotal.gemfirexd.jdbc.ClientDriver| Specify the JDBC driver for SnappyData|
	|snappydata.store.locators|localhost:10334| Specify the URI of the locator (only local/split mode) |
	|master|local[*]| Specify the URI of the spark master (only local/split mode) |
	|zeppelin.jdbc.concurrent.use|true| Specify the Zeppelin scheduler to be used. </br>Select **True** for Fair and **False** for FIFO | 
 
6. Bind the interpreter.</br> You can click on the interpreter to bind or unbind it. Click on the interpreter from the list to bind it to the notebook. Click **Save** to apply your changes.

7. Set SnappyData as the default interpreter.</br> Snappydata Zeppelin Interpreter group consist of two interpreters. Click and drag *<_Interpreter_Name_>* to the top of the list to set it as the default interpreter. Click **Save** to apply your changes.
 	
	| Interpreter Name | Description |
	|--------|--------|
    | %snappydata.snappydata or </br> %snappydata.spark | This interpreter is used to write scala code on the paragraph.SnappyContext is injected in this interpreter and can be accessed using variable **snc** |
    |%snappydata.sql | This interpreter is used to execute sql queries on snappydata cluster. It also has features of executing Approximate queries on snappydata cluster.|

### Examples of Queries and Results
Create Notebooks and [try these examples](../concepts/isight/creating_notebooks/).
