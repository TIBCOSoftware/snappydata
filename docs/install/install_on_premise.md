<a id="getting-started-by-installing-snappydata-on-premise"></a>
# Installing TIBCO ComputeDB On-Premise

TIBCO ComputeDB runs on UNIX-like systems (for example, Linux, Mac OS). With on-premises installation, TIBCO ComputeDB is installed and operated from your in-house computing infrastructure. 
The following instructions can be used for on-premise installation of TIBCO ComputeDB:

1. On the [TIBCO eDelivery website](https://edelivery.tibco.com), search for **TIBCO ComputeDB** and go to the **Product Detail** page.
2. Click **Download** and then enter your credentials. 
3. In the Download page, select the version number and then select **Linux**.
4. Read and accept the **END USER LICENSE AGREEMENT**.
5. From the Installation options, choose **Individual file download**.
6. Click **TIB_compute_cluster_1.2.0_linux.zip** to download the cluster distribution which includes product tarball and Apache Zeppelin zip. Additionally, you can also click **TIB_compute_drivers_1.2.0_linux.zip** to download the client drivers. After **TIB_compute_cluster_1.2.0_linux.zip** is downloaded, run the following commands:

		$ unzip TIB_compute_cluster_1.2.0_linux.zip && rm TIB_compute_cluster_1.2.0_linux.zip
        $ cd TIB_compute_cluster_1.2.0_linux/
        $ tar -xzf TIB_compute_1.2.0_linux.tar.gz
        $ cd TIB_compute_1.2.0_linux/

8.	Create a directory for TIBCO ComputeDB artifacts

            $ mkdir quickstartdatadir
            $./bin/spark-shell --conf spark.snappydata.store.sys-disk-dir=quickstartdatadir --conf spark.snappydata.store.log-file=quickstartdatadir/quickstart.log
    
	It opens the Spark shell. All TIBCO ComputeDB metadata, as well as persistent data, is stored in the directory **quickstartdatadir**.</br>	The spark-shell can now be used to work with TIBCO ComputeDB using [SQL](using_sql.md) and [Scala APIs](using_spark_scala_apis.md).
    
After installing TIBCO ComputeDB, follow the instructions [here](/howto/use_apache_zeppelin_with_snappydata.md), to use the product from Apache Zeppelin.

<a id="singlehost"></a>
## Single-Host Deployment

This is the simplest form of deployment and can be used for testing and POCs.

Open the command prompt, go the location of the downloaded TIBCO ComputeDB file, and run the following command to extract the archive file.

```pre
$ tar -xzf TIB_compute_1.2.0_linux.tar.gz
$ cd TIB_compute_1.2.0_linux/
```

Start a basic cluster with one data node, one lead, and one locator:

```pre
./sbin/snappy-start-all.sh
```

For custom configuration and to start more nodes, refer [configuring the TIBCO ComputeDB cluster](../configuring_cluster/configuring_cluster.md).


<a id="multihost"></a>
## Multi-Host Deployment

For real-life use cases, you require multiple machines on which TIBCO ComputeDB must be deployed. You can start one or more TIBCO ComputeDB node on a single machine based on your machine size.

Where there are multiple machines involved, you can deploy TIBCO ComputeDB on:

*	[Machines With Shared Path](#sharedpath)

*	[Machines Without a Shared Path](#machine-shared-path)

*	[Machines Without Passwordless SSH](#without_passwordless)

<a id="sharedpath"></a>
### Machines With a Shared Path
If all the machines in your cluster can share a path over an NFS or similar protocol, then use the following instructions:

**Prerequisites**

* Ensure that the **/etc/hosts** correctly configures the host and IP address of each TIBCO ComputeDB member machine.

* Ensure that SSH is supported and you have configured all the machines to be accessed by [passwordless SSH](../reference/misc/passwordless_ssh.md). If SSH is not supported then follow the instructions in the [Machines Without Passwordless SSH](#without_passwordless) section.

**To set up the cluster for machines with a shared path:**

1. Copy the downloaded binaries to the shared folder.

2. Extract the downloaded archive file and go to TIBCO ComputeDB home directory.

		$ tar -xzf TIB_compute_1.2.0_linux.tar.gz
		$ cd TIB_compute_1.2.0_linux/

3. Configure the cluster as described in [Configuring the Cluster](../configuring_cluster/configuring_cluster.md).

4. After configuring each of the members in the cluster, run the `snappy-start-all.sh` script:

		./sbin/snappy-start-all.sh

	This creates a default folder named **work** and stores all TIBCO ComputeDB member's artifacts separately. The folder is identified by the name of the node.

!!!Tip
	For optimum performance, configure the **-dir** to a local directory and not to a network directory. When **-dir** property is configured for each member in the cluster, the artifacts of the respective members get created in the  **-dir** folder.

<a id="machine-shared-path"></a>
### Machines Without a Shared Path

In case all the machines in your cluster do not share a path over an NFS or similar protocol, then use the following instructions:

**Prerequisites**

*	Ensure that **/etc/hosts** correctly configures the host and IP Address of each TIBCO ComputeDB member machine.

*	Ensure that SSH is supported and you have configured all the machines to be accessed by [passwordless SSH](../reference/misc/passwordless_ssh.md). If SSH is not supported then follow the instructions in the [Machines without passwordless SSH](#without_passwordless) section.

**To set up the cluster for machines without a shared path:**

1.	Copy and extract the downloaded binaries into each machine.	Ensure to maintain the same directory structure on all the machines. For example, if you copy the binaries in **/opt/computedb/** on the first machine, then you must ensure to copy the binaries to **/opt/computedb/** on rest of the machines.

2.	Configure the cluster as described in [Configuring the Cluster](../configuring_cluster/configuring_cluster.md). Maintain one node as the controller node, where you can configure your cluster. Usually this is done in the lead node. On that machine, you can edit files such as servers, locators, and leads which are in the **$SNAPPY_HOME/conf/ directory**.

3.	Create a working directory on every machine, for each of the TIBCO ComputeDB member that you want to run. <br> The member's working directory provides a default location for the logs, persistence, and status files of that member. <br>For example, if you want to run both a locator and server member on the local machine, create separate directories for each member.

4.	Run the `snappy-start-all.sh` script:

		./sbin/snappy-start-all.sh

<a id="without_passwordless"></a>
### Machines Without Passwordless SSH

In case the machines in your cluster do not share a common path as well as cannot be accessed by [passwordless SSH](../reference/misc/passwordless_ssh.md), then you can use the following instructions to deploy TIBCO ComputeDB:

**To set up the cluster for machines without passwordless SSH:**

1.	Copy and extract the downloaded binaries into each machine. The binaries can be placed in different directory structures. 

3.	Configure each member separately.

	!!!Note
			The scripts used for starting individual members in the cluster do not read from the **conf** file of each member, hence there is no need to edit the **conf** files for starting the members. These scripts will start the member with the default configuration properties. To override the default configuration, you can pass the properties as arguments to the above scripts.

5.	Start the members in the cluster one at a time. Start the locator first, then the servers, and finally the leads. Use the following scripts to start the members:

	*	`$SNAPPY_HOME/sbin/snappy-locator.sh`

	*	`$SNAPPY_HOME/sbin/snappy-server.sh`
	
	*	`$SNAPPY_HOME/sbin/snappy-lead.sh`


    **Start Examples**:

    	    $SNAPPY_HOME/sbin/snappy-locator.sh start -dir=/tmp/locator
            $SNAPPY_HOME/sbin/snappy-server.sh  start -dir=/tmp/server -locators="localhost:10334"
            $SNAPPY_HOME/sbin/snappy-lead.sh      start -dir=/tmp/lead      -locators="localhost:10334"

    **Stop Examples**:

    	    $SNAPPY_HOME/sbin/snappy-locator.sh stop -dir=/tmp/locator
			$SNAPPY_HOME/sbin/snappy-server.sh  stop -dir=/tmp/server
			$SNAPPY_HOME/sbin/snappy-lead.sh      stop -dir=/tmp/lead

 



