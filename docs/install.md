#Overview
The following installation options are available:

* [Install On Premise](#install-on-premise)
* [Setting up Cluster on Amazon Web Services (AWS)](#setting-up-cluster-on-amazon-web-services-aws)
* [Building from Source](#building-from-source)


## Install On-Premise
SnappyData runs on UNIX-like systems (for example, Linux, Mac OS). With on-premises installation, SnappyData is installed and operated from your in-house computing infrastructure.

### Prerequisites
Before you start the installation, make sure that Java SE Development Kit 8 is installed, and the _JAVA_HOME_ environment variable is set on each computer.

### Download SnappyData
Download the latest version of SnappyData from the [SnappyData Release](https://github.com/SnappyDataInc/snappydata/releases/) page, which lists the latest and previous releases of SnappyData.

The packages are available in compressed files (.zip and .tar format). On this page, you can also view details of features and enhancements introduced in specific releases.

* ** SnappyData 0.7 download link **
[(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-bin.zip)

* **SnappyData 0.7 (hadoop provided) download link** [(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-without-hadoop-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.7/snappydata-0.7-without-hadoop-bin.zip)
<a id="singlehost"></a>
### Single Host Installation
This is the simplest form of deployment and can be used for testing and POCs.

Open the command prompt and run the following command to extract the downloaded archive file and to go the location of the SnappyData home directory. 
```bash
$ tar -xzf snappydata-0.7-bin.tar.gz   
$ cd snappydata-0.7-bin/
```
Start a basic cluster with one data node, one lead, and one locator
```
./sbin/snappy-start-all.sh
```
For custom configuration and to start more nodes,  see the section [How to Configure the SnappyData cluster](configuration.md)

### Multi-Host Installation
For real life use cases, you need multiple machines on which SnappyData can be deployed. You can start one or more SnappyData node on a single machine based on your machine size.

### Machines with a Shared Path
If all your machines can share a path over an NFS or similar protocol, then follow the steps below:

#### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP address of each SnappyData member machine.

* Ensure that SSH is supported and you have configured all machines to be accessed by [passwordless SSH](configuration#configuring-ssh-login-without-password).

#### Steps to Set up the Cluster

1. Copy the downloaded binaries to the shared folder.

2. Extract the downloaded archive file and go to SnappyData home directory.

		$ tar -xzf snappydata-0.7-bin.tar.gz 
		$ cd snappydata-0.7-bin/
 
3. Configure the cluster as described in [How to Configure SnappyData cluster](configuration.md).

4. After configuring each of the components, run the `snappy-start-all.sh` script:

		./sbin/snappy-start-all.sh 

This creates a default folder named **work** and stores all SnappyData member's artifacts separately. Each member folder is identified by the name of the node.

If SSH is not supported then follow the instructions in the Machines without a Shared Path section.

### Machines without a Shared Path 

#### Prerequisites

* Ensure that the **/etc/hosts** correctly configures the host and IP Address of each SnappyData member machine.

* On each host machine, create a new member working directory for each SnappyData member, that you want to run the host. <br> The member working directory provides a default location for the log, persistence, and status files for each member, and is also used as the default location for locating the member's configuration files.
<br>For example, if you want to run both a locator and server member on the local machine, create separate directories for each member.

#### To Configure the Cluster
1. Copy and extract the downloaded binaries on each machine

2. Individually configure and start each member

<Note> Note: We are providing all configuration parameter as command line arguments rather than reading from a conf file.</Note>

The example below starts a locator and server.

```bash 
$ bin/snappy locator start  -dir=/node-a/locator1 
$ bin/snappy server start  -dir=/node-b/server1  -locators:localhost:10334

$ bin/snappy locator stop
$ bin/snappy server stop
``` 

## Setting up Cluster on Amazon Web Services (AWS)


### Using AWS Management Console
You can launch a SnappyData cluster on Amazon EC2 instance(s) using AMI provided by SnappyData. For more information on launching an EC2 instance, refer to the [AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html).

#### Prerequisites
Ensure that you have an existing AWS account with required permissions to launch EC2 resources

#### Launching the Instance
To launch the instance and start SnappyData cluster:

1. Open the [Amazon EC2 console](https://console.aws.amazon.com/ec2/) and sign in using your AWS login credentials.

2. The current region is displayed at the top of the screen. Select the region where you want to launch the instance.

3. Click **Launch Instance** from the Amazon EC2 console dashboard.

4. On the **Choose an Amazon Machine Image (AMI)** page, select **Community AMIs** from the left pane.

5. Enter **SnappyData** in the search box, and press **Enter** on your keyboard. 

6. The search result is displayed. From the search results, click **Select** to choose the AMI with the latest release version.

7. On the **Choose an Instance Type** page, select the instance type as per the requirement of your use case and then click **Review and Launch** to launch the instance with default configurations. <br/>
	<note> Note: </note>
	
	* <note>You can also continue customizing your instance before you launch the instance. Refer to the AWS documentation for more information.</note>
	* <note> When configuring the security groups, ensure that you open at least ports 22 (for SSH access to the EC2 instance) and 5050 (for access to Snappy UI).</note>

8. You are directed to the last step **Review Instance Launch**. Check the details of your instance, and click **Launch**.

9. In the **Select an existing key pair or create a new key pair** dialog box, select a key pair.

10. Click **Launch** to launch the instances.

9. The dashboard which lists the instances is displayed. Click **Refresh** to view the updated list and the status of the instance creation.

10. Once the status of the instance changes to **running**, you have successfully created and launched the instance with the SnappyData AMI.

12. Use SSH to connect to the instance using the **ubuntu** username. You require:

	* The private key file of the key pair with which the instance was launched, and 

	* Details of the public hostname or IP address of the instance. Refer to the following documentation, for more information on [accessing an EC2 instance](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html). 

	<note> Note: </note>
	
	* <note>The public hostname/IP address information is available on the EC2 dashboard > **Description** tab. </note>

	* <note> The SnappyData binaries are automatically downloaded and extracted to the location **/snappydata/downloads/** and Java 8 is installed. </note>

13. Follow the [steps described here](#install-on-premise) to continue. </br>


<a id="EC2"></a>
### Using SnappyData EC2 Scripts

The `snappy-ec2` script enables users to quickly launch and manage SnappyData clusters on Amazon EC2. You can also configure the individual nodes of the cluster by providing properties in specific conf files which the script reads before launching the cluster.

The `snappy-ec2` script has been derived from the `spark-ec2` script available in [Apache Spark 1.6](https://github.com/apache/spark/tree/branch-1.6/ec2).

The scripts are available on GitHub in the [snappy-cloud-tools repository](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/aws/ec2) and also as a [**.tar.gz**](https://github.com/SnappyDataInc/snappy-cloud-tools/releases) file.

<Note> Note: The EC2 script is under development. Feel free to try it out and provide your feedback.</Note>


#### Prerequisites

* Ensure that you have an existing AWS account with required permissions to launch EC2 resources

* Create an EC2 Key Pair in the region where you want to launch the SnappyData Cloud cluster
<br/>Refer to the Amazon Web Services EC2 documentation for more information on [generating your own EC2 Key Pair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html).

* Using the AWS Secret Access Key and the Access Key ID, set the two environment variables, `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID`. You can find this information in the AWS IAM console page.<br/> 
If you already have set up the AWS Command Line Interface on your local machine, the script automatically detects and uses the credentials from the AWS credentials file.

	For example:
```export AWS_SECRET_ACCESS_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112```
```export AWS_ACCESS_KEY_ID=A1B2C3D4E5F6G7H8I9J10```

* Ensure Python v 2.7 or later is installed on your local computer.


#### Cluster Management

##### Launching SnappyData Cluster

In the command prompt, go to the directory where the **snappydata-ec2-`<version>`.tar.gz** is extracted or to the aws/ec2 directory where the [SnappyData cloud tools repository](https://github.com/SnappyDataInc/snappydata-cloud-tools) is cloned locally.

Enter the command in the following format.

`./snappy-ec2 -k <your-key-name> -i <your-keyfile-path> <action> <your-cluster-name>`

Here, `<your-key-name>` refers to the EC2 Key Pair, `<your-keyfile-path>` refers to the path to the key file and `<action>` refers to the action to be performed (for example, launch, start, stop).
 
By default, the script starts one instance of a locator, lead and server each.
The script identifies each cluster by its unique cluster name (you provided) and internally ties members (locators, leads, and stores/servers) of the cluster with EC2 security groups. 

The  names and details of the members are automatically derived from the provided cluster name. 

For example, if you launch a cluster named **my-cluster**, the locator is available in security group named **my-cluster-locator** and the store/server are available in **my-cluster-store**.

When running the script you can also specify properties like the number of stores and region.

For example, using the following command, you can start a SnappyData cluster named **snappydata-cluster** with 2 stores (or servers) in the N. California (us-west-1) region on AWS. It also starts an Apache Zeppelin server on the instance where lead is running.

The examples below assume that you have the key file (my-ec2-key.pem) in your home directory for EC2 Key Pair named 'my-ec2-key'.

```
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem --stores=2 --with-zeppelin=embedded --region=us-west-1 launch snappydata-cluster 
```
To start Apache Zeppelin on a separate instance, use `--with-zeppelin=non-embedded`. 

##### Specifying Properties

If you want to configure each of the locator, lead or server with specific properties, you can do so by specifying them in files named **locators**, **leads** or **servers**, respectively and placing these under aws/ec2/deploy/home/ec2-user/snappydata/. Refer to [this SnappyData documentation page](configuration/#configuration-files) for example on how to write these conf files.

This is similar to how one would provide properties to SnappyData cluster nodes while launching it using the `sbin/snappy-start-all.sh` script.

The important difference here is that, instead of the host names of the locator, lead or store, you have to write {{LOCATOR_N}}, {{LEAD_N}} or {{SERVER_N}} in these files, respectively. N stands for Nth locator, lead or server. The script replaces these with the actual host name of the members when they are launched.

The sample conf files for a cluster with 2 locators, 1 lead and 2 stores are given below:

*locators*
````
{{LOCATOR_0}} -peer-discovery-port=9999 -heap-size=1024m
{{LOCATOR_1}} -peer-discovery-port=9888 -heap-size=1024m
````
*leads*
````
{{LEAD_0}} -heap-size=4096m -J-XX:MaxPermSize=512m -spark.ui.port=3333 -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -spark.executor.cores=10
````
*servers*
````
{{SERVER_0}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888
{{SERVER_1}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -client-port=1530
````
When you run **snappy-ec2**, it looks for these files under **aws/ec2/deploy/home/ec2-user/snappydata/** and, if present, reads them while launching the cluster on Amazon EC2. Ensure that the number of locators, leads or servers specified by options `--locators`, `--leads` or `--stores` must match to the number of entries in their respective conf files.

The script also reads **snappy-env.sh**, if present in this location.

##### Stopping a Cluster

When you stop a cluster, it shuts down the EC2 instances and any data saved on its local instance stores is lost. However, the data saved on EBS volumes, if any, is retained unless the spot-instances were used.
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem stop cluster-name
````

##### Starting a Cluster

When you start a cluster, it uses the existing EC2 instances associated with the cluster name and launches SnappyData processes on them.
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem start cluster-name
````
<Note>Note: The start command (or launch command with --resume option) ignores --locators, --leads or --stores options and launches SnappyData cluster on existing instances. But the conf files are read in any case if they are present in the location mentioned above. So you need to ensure that every time you use start command, the number of entries in conf files are equal to the number of instances in their respective security group.
</Note>
##### Adding Servers to  a Cluster

This is not yet fully supported via the script. You may have to manually launch an instance with `(cluster-name)-stores` group and then use launch command with --resume option.

##### Listing Members of a Cluster

**To get the first locator's hostname:**
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem get-locator cluster-name
````
**To get the first lead's hostname, use the get-lead command:**

##### Connecting to a Cluster

You can connect to any instance of a cluster with SSH using the login command. It logs you into the first lead instance. From there, you can SSH to any other member of the cluster without a password.
The SnappyData product directory is located under /home/ec2-user/snappydata/ on all the members.
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem login cluster-name
````

##### Destroying a Cluster

Destroying a cluster destroys all the data on the local instance stores as well as on the attached EBS volumes permanently.
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem destroy cluster-name
````
This retains the security groups created for this cluster. To delete them as well, use it with --delete-group option.

##### Starting Cluster with Apache Zeppelin

Optionally, you can start an instance of Apache Zeppelin server with the cluster. [Apache Zeppelin](https://zeppelin.apache.org/) is a web-based notebook that enables interactive notebook. You can start it either on a lead node's instance (`--with-zeppelin=embedded`)  or on a separate instance (`--with-zeppelin=non-embedded`).
````
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem --with-zeppelin=embedded launch cluster-name
````

#### More options

For a complete list of options this script has, run `./snappy-ec2`. The options are also provided below for quick reference.
````
Usage: `snappy-ec2 [options] <action> <cluster_name>`

<action> can be: launch, destroy, login, stop, start, get-locator, get-lead, reboot-cluster

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -s STORES, --stores=STORES
                        Number of stores to launch (default: 1)
  --locators=LOCATORS   Number of locator nodes to launch (default: 1)
  --leads=LEADS         Number of lead nodes to launch (default: 1)
  -w WAIT, --wait=WAIT  DEPRECATED (no longer necessary) - Seconds to wait for
                        nodes to start
  -k KEY_PAIR, --key-pair=KEY_PAIR
                        Key pair to use on instances
  -i IDENTITY_FILE, --identity-file=IDENTITY_FILE
                        SSH private key file to use for logging into instances
  -p PROFILE, --profile=PROFILE
                        If you have multiple profiles (AWS or boto config),
                        you can configure additional, named profiles by using
                        this option (default: none)
  -t INSTANCE_TYPE, --instance-type=INSTANCE_TYPE
                        Type of instance to launch (default: m3.large).
                        WARNING: must be 64-bit; small instances won't work
  --locator-instance-type=LOCATOR_INSTANCE_TYPE
                        Locator instance type (leave empty for same as
                        instance-type)
  -r REGION, --region=REGION
                        EC2 region used to launch instances in, or to find
                        them in (default: us-east-1)
  -z ZONE, --zone=ZONE  Availability zone to launch instances in, or 'all' to
                        spread stores across multiple (an additional $0.01/Gb
                        for bandwidthbetween zones applies) (default: a single
                        zone chosen at random)
  -a AMI, --ami=AMI     Amazon Machine Image ID to use
  -v SNAPPYDATA_VERSION, --snappydata-version=SNAPPYDATA_VERSION
                        Version of SnappyData to use: 'X.Y.Z' (default:
                        LATEST)
  --with-zeppelin=WITH_ZEPPELIN
                        Launch Apache Zeppelin server with the cluster. Use
                        'embedded' to launch it on lead node and 'non-
                        embedded' to launch it on a separate instance.
  --deploy-root-dir=DEPLOY_ROOT_DIR
                        A directory to copy into/on the first master. Must
                        be absolute. Note that a trailing slash is handled as
                        per rsync: If you omit it, the last directory of the
                        --deploy-root-dir path will be created in / before
                        copying its contents. If you append the trailing
                        slash, the directory is not created and its contents
                        are copied directly into /. (default: none).
  -D [ADDRESS:]PORT     Use SSH dynamic port forwarding to create a SOCKS
                        proxy at the given local address (for use with login)
  --resume              Resume installation on a previously launched cluster
                        (for debugging)
  --ebs-vol-size=SIZE   Size (in GB) of each EBS volume.
  --ebs-vol-type=EBS_VOL_TYPE
                        EBS volume type (e.g. 'gp2', 'standard').
  --ebs-vol-num=EBS_VOL_NUM
                        Number of EBS volumes to attach to each node as
                        /vol[x]. The volumes will be deleted when the
                        instances terminate. Only possible on EBS-backed AMIs.
                        EBS volumes are only attached if --ebs-vol-size > 0.
                        Only support up to 8 EBS volumes.
  --placement-group=PLACEMENT_GROUP
                        Which placement group to try and launch instances
                        into. Assumes placement group is already created.
  --spot-price=PRICE    If specified, launch stores as spot instances with the
                        given maximum price (in dollars)
  -u USER, --user=USER  The SSH user you want to connect as (default:
                        ec2-user)
  --delete-groups       When destroying a cluster, delete the security groups
                        that were created
  --use-existing-locator
                        Launch fresh stores, but use an existing stopped
                        locator if possible
  --user-data=USER_DATA
                        Path to a user-data file (most AMIs interpret this as
                        an initialization script)
  --authorized-address=AUTHORIZED_ADDRESS
                        Address to authorize on created security groups
                        (default: 0.0.0.0/0)
  --additional-security-group=ADDITIONAL_SECURITY_GROUP
                        Additional security group to place the machines in
  --additional-tags=ADDITIONAL_TAGS
                        Additional tags to set on the machines; tags are
                        comma-separated, while name and value are colon
                        separated; ex: "Task:MySnappyProject,Env:production"
  --subnet-id=SUBNET_ID
                        VPC subnet to launch instances in
  --vpc-id=VPC_ID       VPC to launch instances in
  --private-ips         Use private IPs for instances rather than public if
                        VPC/subnet requires that.
  --instance-initiated-shutdown-behavior=INSTANCE_INITIATED_SHUTDOWN_BEHAVIOR
                        Whether instances should terminate when shut down or
                        just stop
  --instance-profile-name=INSTANCE_PROFILE_NAME
                        IAM profile name to launch instances under
````


### Limitations
Some of the known limitations of the script are:

* Launching the cluster on custom AMI (specified via --ami option) will not work if it does not have the user 'ec2-user' with sudo permissions

* Support for option --user is incomplete

## Building from Source
Building SnappyData requires JDK 7+ installation ([Oracle Java SE](http://www.oracle.com/technetwork/java/javase/downloads/index.html)). 

Quickstart to build all components of SnappyData:

**Latest release branch**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.7 --recursive
> cd snappydata
> ./gradlew product
```

**Master**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git --recursive
> cd snappydata
> ./gradlew product
```

The product is in **build-artifacts/scala-2.11/snappy**

If you want to build only the top-level SnappyData project but pull in jars for other projects (_spark_, _store_, _spark-jobserver_):

**Latest release branch**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.7
> cd snappydata
> ./gradlew product
```

**Master**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git
> cd snappydata
> ./gradlew product
```


### Repository Layout

- **core** - Extensions to Apache Spark that should not be dependent on SnappyData Spark additions, job server etc. It is also the bridge between _spark_ and _store_ (GemFireXD). For example, SnappyContext, row and column store, streaming additions etc.

- **cluster** - Provides the SnappyData implementation of cluster manager embedding GemFireXD, query routing, job server initialization etc.

  This component depends on _core_ and _store_.  The code in the _cluster_ depends on _core_ but not the other way round.

- **spark** - _Apache Spark_ code with SnappyData enhancements.

- **store** - Fork of gemfirexd-oss with SnappyData additions on the snappy/master branch.

- **spark-jobserver** - Fork of _spark-jobserver_ project with some additions to integrate with SnappyData.

  The _spark_, _store_, and _spark-jobserver_ directories are required to be clones of the respective SnappyData repositories and are integrated into the top-level SnappyData project as git sub-modules. When working with sub-modules, updating the repositories follows the normal [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules). One can add some aliases in gitconfig to aid pull/push like:

```
[alias]
  spull = !git pull && git submodule sync --recursive && git submodule update --init --recursive
  spush = push --recurse-submodules=on-demand
```

The above aliases can serve as useful shortcuts to pull and push all projects from top-level _snappydata_ repository.


### Building

Gradle is the build tool used for all the SnappyData projects. Changes to _Apache Spark_ and _spark-jobserver_ forks include the addition of Gradle build scripts to allow building them independently as well as a subproject of SnappyData. The only requirement for the build is a JDK 7+ installation. Currently, most of the testing has been with JDK 7. The Gradle wrapper script downloads all the other build dependencies as required.

If a user does not want to deal with submodules and only work on SnappyData project, then can clone only the SnappyData repository (without the --recursive option) and the build will pull those SnappyData project jar dependencies from maven central.

If working on all the separate projects integrated inside the top-level SnappyData clone, the Gradle build will recognize the same and build those projects too and include the same in the top-level product distribution jar. The _spark_ and _store_ submodules can also be built and published independently.

Useful build and test targets:
```
./gradlew assemble      -  build all the sources
./gradlew testClasses   -  build all the tests
./gradlew product       -  build and place the product distribution
                           (in build-artifacts/scala_2.11/snappy)
./gradlew distTar       -  create a tar.gz archive of product distribution
                           (in build-artifacts/scala_2.11/distributions)
./gradlew distZip       -  create a zip archive of product distribution
                           (in build-artifacts/scala_2.11/distributions)
./gradlew buildAll      -  build all sources, tests, product, packages (all targets above)
./gradlew checkAll      -  run testsuites of snappydata components
./gradlew cleanAll      -  clean all build and test output
./gradlew runQuickstart -  run the quickstart suite (the "Getting Started" section of docs)
./gradlew precheckin    -  cleanAll, buildAll, scalaStyle, build docs,
                           and run full snappydata testsuite including quickstart
./gradlew precheckin -Pstore  -  cleanAll, buildAll, scalaStyle, build docs,
                           run full snappydata testsuite including quickstart
                           and also full SnappyData store testsuite
```

The default build directory is _build-artifacts/scala-2.11_ for projects. An exception is _store_ project, where the default build directory is _build-artifacts/&lt;os&gt;_ where _&lt;os&gt;_ is _linux_ on Linux systems, _osx_ on Mac, _windows_ on Windows.

The usual Gradle test run targets (_test_, _check_) work as expected for JUnit tests. Separate targets have been provided for running Scala tests (_scalaTest_) while the _check_ target runs both the JUnit and ScalaTests. One can run a single Scala test suite class with _singleSuite_ option while running a single test within some suite works with the _--tests_ option:

```sh
> ./gradlew core:scalaTest -PsingleSuite=**.ColumnTableTest  # run all tests in the class
> ./gradlew core:scalaTest \
>    --tests "Test the creation/dropping of table using SQL"  # run a single test (use full name)
```
Running individual tests within some suite works using the _--tests_ argument.


### Setting up IntelliJ with Gradle

IntelliJ is the IDE commonly used by the SnappyData developers. Those who really prefer Eclipse can try the Scala-IDE and Gradle support but has been seen to not work as well (for example, Gradle support is not integrated with Scala plugin etc).  

To import into IntelliJ:

* Update IntelliJ to the latest 14.x (or 15.x) version, including the latest Scala plug-in. Older versions have trouble dealing with Scala code particularly, some of the code in Spark.

* Select **Import Project**, and then point to the SnappyData directory. Use external Gradle import. When using JDK 7, add **-XX:MaxPermSize=350m** to VM options in global Gradle settings. Select the default values, and click **Next** in the following screens.<br/> 
<note> Note:</note> 

	- <note>Ignore the **"Gradle location is unknown warning"**.</note> 
	- <note>Ensure that a JDK 7/8 installation has been selected.</note> 
	- <note>Ignore and dismiss the **"Unindexed remote maven repositories found"** warning message if seen.</note>

* When import is completed, go to **File> Settings> Editor> Code Style> Scala**. Set the scheme as **Project**. Check that the same has been set in Java Code Style too. Click OK to apply and close it. Next, copy **codeStyleSettings.xml** located in the SnappyData top-level directory, to the **.idea** directory created by IntelliJ. Check that the settings are now applied in **File> Settings> Editor> Code Style> Java** which should display Indent as 2 and continuation indent as 4 (same as Scala).

* If the Gradle tab is not visible immediately, then select it from window list pop-up at the left-bottom corner of IDE. If you click on that window list icon, then the tabs are displayed permanently.

* Generate Apache Avro and GemFireXD required sources by expanding: **snappydata_2.11> Tasks> other**. Right-click on **generateSources** and run it. The Run option may not be available if indexing is still in progress, wait for indexing to complete, and then try again. <br> The first run may some time to complete,  as it downloads jar files and other required files. This step has to be done the first time, or if **./gradlew clean** has been run, or if you have made changes to **javacc/avro/messages.xml** source files. 

* If you get unexpected **"Database not found"** or **NullPointerException** errors in GemFireXD layer, then first thing to try is to run the **generateSources** target again.*

* Increase the compiler heap sizes or else the build can take quite long especially with integrated **spark** and **store**. In **File> Settings> Build, Execution, Deployment> Compiler increase**, **Build process heap size** to say 1536 or 2048. Similarly, increase JVM maximum heap size in **Languages & Frameworks> Scala Compiler Server** to 1536 or 2048.

* Test the full build.

* For JDK 7: **Open Run> Edit Configurations**. Expand **Defaults**, and select **Application**. Add **-XX:MaxPermSize=350m** in the VM options. Similarly, add it to VM parameters for ScalaTest and JUnit. Most of the unit tests have trouble without this option.

* For JUnit configuration also append **/build-artifacts** to the working directory. That is, the directory should be **\$MODULE_DIR\$/build-artifacts**. Likewise change working directory for ScalaTest to be inside **build-artifacts** otherwise, all intermediate log and other files (specially created by GemFireXD) pollute the source tree and may need to be cleaned manually.


### Running a ScalaTest/JUnit

Running Scala/JUnit tests from IntelliJ is straightforward. Ensure that **MaxPermSize** has been increased when using JDK 7 as mentioned above especially for Spark/Snappy tests.

* When selecting a run configuration for JUnit/ScalaTest, avoid selecting the Gradle one (green round icon) otherwise an external Gradle process is launched that can start building the project again and won't be cleanly integrated with IntelliJ. Use the normal JUnit (red+green arrows icon) or ScalaTest (JUnit like with red overlay).

* For JUnit tests, ensure that working directory is **\$MODULE_DIR\$/build-artifacts** as mentioned before. Otherwise, many GemFireXD tests will fail to find the resource files required in tests. They also pollute the files etc, so when launched this will allow those to go into build-artifacts that is easier to clean. For that reason, it may be preferable to do the same for ScalaTests.

* Some of the tests use data files from the **tests-common** directory. For such tests, run the Gradle task **snappydata_2.11> Tasks> other> copyResourcesAll** to copy the resources in build area where IntelliJ runs can find it.