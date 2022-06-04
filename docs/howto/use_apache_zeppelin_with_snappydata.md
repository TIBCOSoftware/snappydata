<a id="howto-zeppelin"></a>
# Using Apache Zeppelin with SnappyData


## Step 1: Download, Install and Configure SnappyData

1. [Download and install SnappyData](../install/index.md)

2. [Configure the SnappyData Cluster](../configuring_cluster/configuring_cluster.md)

3. [Start the SnappyData cluster](start_snappy_cluster.md)

4. Extract the contents of the [Zeppelin 0.8.2 binary package](http://archive.apache.org/dist/zeppelin/zeppelin-0.8.2/zeppelin-0.8.2-bin-netinst.tgz).
   Then `cd` into the extracted `zeppelin-0.8.2-bin-netinst` directory.</br>
   Note that while these instructions work with any version of Zeppelin, the demo notebooks installed later
   have been created and tested only on Zeppelin 0.8.2 and may not work correctly on other versions.

5. Install a couple of additional interpreters (angular is used by display panels of the sample notebooks installed later):
    ```
    ZEPPELIN_INTERPRETER_DEP_MVNREPO=https://repo1.maven.org/maven2 ./bin/install-interpreter.sh --name angular,jdbc
    ```
   If you are using the `all` binary package from zeppelin instead of the `netinst` package linked in the previous step,
   then you can skip this step.

6. Copy the [SnappyData JDBC client jar](https://github.com/TIBCOSoftware/snappydata/releases/download/v1.3.1/snappydata-jdbc_2.11-1.3.1.jar)
   inside the `interpreter/jdbc` directory.

7. Download the predefined SnappyData notebooks with configuration [notebooks\_embedded\_zeppelin.tar.gz](https://github.com/TIBCOSoftware/snappy-zeppelin-interpreter/blob/master/examples/notebook/notebooks_embedded_zeppelin.tar.gz). </br>
   Extract the contents of the compressed tar file (tar xzf) in the Zeppelin installation on your local machine.

8. Start the Zeppelin daemon using the command: </br> `./bin/zeppelin-daemon.sh start`

9. To ensure that the installation is successful, log into the Zeppelin UI (**http://localhost:8080** or <AWS-AMI\_PublicIP>:8080) from your web browser.

![homepage](../Images/zeppelin.png)

Refer [here](concurrent_apache_zeppelin_access_to_secure_snappydata.md) for instructions to configure Apache Zeppelin for securely accessing SnappyData Cluster.


## Step 2: Configure Interpreter Settings

1. Log on to Zeppelin from your web browser and select **Interpreter** from the **Settings** option.
   This will require a user having administrator privileges, which is set to `admin` by default.
   See **zeppelin-dir/conf/shiro.ini** file for the default admin password and other users and
   update the file to use your preferred authentication scheme as required.

2. Click on **edit** in the `jdbc` interpreter section.

3. Configure the interpreter properties. </br>The table below lists the properties required for SnappyData.

   | Property    | Value | Description |
   |-------------|-------|-------------|
   |default.driver               |io.snappydata.jdbc.ClientDriver  |Specify the JDBC driver for SnappyData |
   |default.url                  |jdbc:snappydata://localhost:1527 |Specify the JDBC URL for SnappyData cluster in the format `jdbc:snappydata://<locator_hostname>:1527` |
   |default.user                 |SQL user name or `app`           |If security is enabled in the SnappyData cluster, then the configured user name else `app` |
   |default.password             |SQL user password or `app`       |If security is enabled in the SnappyData cluster, then the password of the user else can be anything |
   |default.splitQueries         |true                             |Each query in a paragraph is executed apart and returns the result |
   |zeppelin.jdbc.concurrent.use |true                             |Specify the Zeppelin scheduler to be used. </br>Select **True** for Fair and **False** for FIFO |
   |zeppelin.jdbc.interpolation  |true                             |If interpolation of `ZeppelinContext` objects into the paragraph text is allowed |

4. If required, edit other properties, and then click **Save** to apply your changes.</br>


## FAQs

* **I am on the homepage, what should I do next?**
    *	If you are using SnappyData for the first time, you can start with the QuickStart notebooks to start exploring the capabilities of the product.
    *	If you have used SnappyData earlier and just want to explore the new interface, you can download data from external sources using the notebooks in the External Data Sources section.

* **I get an error when I run a paragraph?**
    *	By design, the anonymous user is not allowed to execute notebooks.
    *	You may clone the notebook and proceed in the cloned notebook.

* **Do I need to change any setting in Zeppelin to work with the multi-node cluster?**
    *	Yes, but this requires Zeppelin’s admin user access. By default, you access the Zeppelin notebooks as an anonymous user. For admin user access, click the **Interpreter** tab and enter your credentials in the **Login** box. You can find the admin credentials in the **zeppelin-dir/conf/shiro.ini** file.
    ![homepage](../Images/zeppelin_3.png)
    *	Update the appropriate IP of a server node in the jdbc URL (highlighted in the following image).
    ![homepage](../Images/zeppelin_2.png)

* **Do these notebooks depend on specific Zeppelin version?**
    Yes, these notebooks were developed on Zeppelin version 0.8.2.

* **Are there any configuration files of Zeppelin that I need to be aware of?**
    For advanced multi-user settings, refer to the **zeppelin-site.xml** and **shiro.ini**. For more details and options,  refer to the Apache Zeppelin documentation.

* **Is Zeppelin the only interface to interact with SnappyData?**
    No, if you prefer a command-line interface, then the product provides two command-line interfaces. The SQL interface, which can be accessed using **./bin/snappy** and the experimental scala interpreter can be invoked using **./bin/snappy-scala**.
    You can also use standard JDBC tools like SQuirreL SQL, DbVisualizer, DBeaver etc.
    See the respective tool documentation to configure SnappyData JDBC driver (snappydata-jdbc_2.11-1.3.1.jar).
    For BI tools, refer to [TIBCO Spotfire®](connecttibcospotfire.md) and [Tableau](tableauconnect.md).

* **How to configure Apache Zeppelin to securely and concurrently access the SnappyData Cluster?**
    Refer to [How to Configure Apache Zeppelin to Securely and Concurrently access the SnappyData Cluster](concurrent_apache_zeppelin_access_to_secure_snappydata.md).
