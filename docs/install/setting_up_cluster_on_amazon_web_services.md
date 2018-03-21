<a id="setting-up-cluster-on-amazon-web-services-aws"></a>
# Setting up Cluster on Amazon Web Services (AWS)

## Using AWS management console
You can launch a SnappyData cluster on Amazon EC2 instance(s) using the AMI provided by SnappyData. For more information on launching an EC2 instance, refer to the [AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/launching-instance.html).

### Prerequisites
Ensure that you have an existing AWS account with required permissions to launch the EC2 resources.

### Launching the instance
To launch the instance and start the SnappyData cluster:

1. Open the [Amazon EC2 console](https://console.aws.amazon.com/ec2/) and sign in using your AWS login credentials.

2. The current region is displayed at the top of the screen. Select the region where you want to launch the instance.

3. Click **Launch Instance** from the Amazon EC2 console dashboard.

4. On the **Choose an Amazon Machine Image (AMI)** page, select **Community AMIs** from the left pane.

5. Enter **SnappyData** in the search box, and press **Enter** on your keyboard.

6. The search result is displayed. From the search results, click **Select** to choose the AMI with the latest release version.

7. On the **Choose an Instance Type** page, select the instance type as per the requirement of your use case and then click **Review and Launch** to launch the instance with default configurations. <br/>

	!!! Note:  
		* You can also continue customizing your instance before you launch the instance. Refer to the AWS documentation for more information.

		*  When configuring the security groups, ensure that you open at least ports 22 (for SSH access to the EC2 instance) and 5050 (for access to Snappy UI).

8. You are directed to the last step **Review Instance Launch**. Check the details of your instance, and click **Launch**.

9. In the **Select an existing key pair or create a new key pair** dialog box, select a key pair.

10. Click **Launch**. The Launch Status page is displayed.

11. Click **View Instances**. The dashboard which lists the instances is displayed.

12. Click **Refresh** to view the updated list and the status of the instance creation.

13. Once the status of the instance changes to **running**, you have successfully created and launched the instance with the SnappyData AMI.

14. Use SSH to connect to the instance using the **Ubuntu** username. You require:

	* The private key file of the key pair with which the instance was launched, and

	* Details of the public hostname or IP address of the instance.
Refer to the following documentation, for more information on [accessing an EC2 instance](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html).


	!!! Note: 
		* The public hostname/IP address information is available on the EC2 dashboard > **Description** tab. 

		* The SnappyData product distribution is already downloaded and extracted in the **/opt/snappydata** directory and Java 8 is installed. 

13. Go to the **/opt/snappydata** directory. Run the following command to start a basic cluster with one data node, one lead, and one locator.

```no-highlight
./sbin/snappy-start-all.sh
```

<a id="EC2"></a>
## Using SnappyData EC2 scripts

The `snappy-ec2` script enables users to quickly launch and manage SnappyData clusters on Amazon EC2. You can also configure the individual nodes of the cluster by providing properties in specific configuration files, which the script reads before launching the cluster.

The `snappy-ec2` script has been derived from the `spark-ec2` script available in [Apache Spark 1.6](https://github.com/apache/spark/tree/branch-1.6/ec2).

The scripts are available on GitHub in the [snappy-cloud-tools repository](https://github.com/SnappyDataInc/snappy-cloud-tools/tree/master/aws/ec2) and also as a **.tar.gz** file on [the release page](https://github.com/SnappyDataInc/snappy-cloud-tools/releases) file.

!!! Note: 
	The EC2 script is under development. Feel free to try it out and provide your feedback.

### Prerequisites

* Ensure that you have an existing AWS account with required permissions to launch EC2 resources

* Create an EC2 Key Pair in the region where you want to launch the SnappyData Cloud cluster
<br/>Refer to the Amazon Web Services EC2 documentation for more information on [generating your own EC2 Key Pair](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html).

* Using the AWS Secret Access Key and the Access Key ID, set the two environment variables, `AWS_SECRET_ACCESS_KEY` and `AWS_ACCESS_KEY_ID`. You can find this information in the AWS IAM console page.<br/>
If you already have set up the AWS Command Line Interface on your local machine, the script automatically detects and uses the credentials from the AWS credentials file.

For example:
```no-highlight
export AWS_SECRET_ACCESS_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112
export AWS_ACCESS_KEY_ID=A1B2C3D4E5F6G7H8I9J10
```

* Ensure Python v 2.7 or later is installed on your local computer.

### Launching SnappyData Cluster

In the command prompt, go to the directory where the **snappydata-ec2-`<version>`.tar.gz** is extracted or to the aws/ec2 directory where the [SnappyData cloud tools repository](https://github.com/SnappyDataInc/snappy-cloud-tools) is cloned locally.

**Syntax**

`./snappy-ec2 -k <your-key-name> -i <your-keyfile-path> <action> <your-cluster-name>`

Here: 

* `<your-key-name>` refers to the EC2 key pair.

* `<your-keyfile-path>` refers to the path to the key file.

* `<action>` refers to the action to be performed. You must first launch your cluster using the `launch` action. The `start` and `stop` actions can be used to manage the nodes in the cluster.

By default, the script starts one instance of a locator, lead, and server each.
The script identifies each cluster by its unique cluster name (you provided) and internally ties members (locators, leads, and stores/servers) of the cluster with EC2 security groups.

The  names and details of the members are automatically derived from the provided cluster name. When running the script you can also specify properties like the number of stores and region.

**Example**

```no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem --stores=2 --with-zeppelin --region=us-west-1 launch my-cluster
```

In the above example, you are launching a SnappyData cluster named **my-cluster** with 2 stores (or servers). The locator is available in security group named **my-cluster-locator** and the store/server are available in **my-cluster-store**.

The cluster is launched in the N. California (us-west-1) region on AWS and starts an Apache Zeppelin server on the instance where the lead is running.</br> 
The example assumes that you have the key file (my-ec2-key.pem) in your home directory for EC2 Key Pair named 'my-ec2-key'.

!!! Note:
	By default, the cluster is launched in the US East (N. Virginia) region on AWS. To launch the cluster in a specific region ensure that you set the region property `--region=`.

### Specifying properties

If you want to configure each of the locator, lead, or server with specific properties, you can do so by specifying them in files named **locators**, **leads** or **servers**, respectively and placing these under **aws/ec2/deploy/home/ec2-user/snappydata/**. Refer to [this SnappyData documentation page](../configuring_cluster/configuring_cluster.md#configuration-files) for example on how to configure these configuration files.</br>
This is similar to how one would provide properties to SnappyData cluster nodes while launching it using the `sbin/snappy-start-all.sh` script.</br>
The important difference here is that, instead of the host names of the locator, lead or store, you have to write {{LOCATOR_N}}, {{LEAD_N}} or {{SERVER_N}} in these files, respectively. N stands for Nth locator, lead or server. The script replaces these with the actual host name of the members when they are launched.

The sample configuration files for a cluster with 2 locators, 1 lead and 2 stores are given below:

*locators*
````no-highlight
{{LOCATOR_0}} -peer-discovery-port=9999 -heap-size=1024m
{{LOCATOR_1}} -peer-discovery-port=9888 -heap-size=1024m
````
*leads*
````no-highlight
{{LEAD_0}} -heap-size=4096m -spark.ui.port=3333 -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -spark.executor.cores=10
````no-highlight
*servers*
````
{{SERVER_0}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888
{{SERVER_1}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -client-port=1530
````
When you run **snappy-ec2**, it looks for these files under **aws/ec2/deploy/home/ec2-user/snappydata/** and, if present, reads them while launching the cluster on Amazon EC2. Ensure that the number of locators, leads or servers specified by options `--locators`, `--leads` or `--stores` must match to the number of entries in their respective configuration file.
</br>The script also reads **snappy-env.sh**, if present in this location.

### Stopping the cluster

When you stop a cluster, it shuts down the EC2 instances and any data saved on the local instance stores is lost. However, the data saved on EBS volumes is retained, unless the spot-instances are used.

````no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem stop cluster-name
````

### Resuming the cluster

When you start a cluster, it uses the existing EC2 instances associated with the cluster name and launches SnappyData processes on them.
````no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem start cluster-name
````
!!!Note: 
	The start command (or launch command with `--resume` option) ignores the `--locators`, `--leads` or `--stores` options, and launches the SnappyData cluster on existing instances. The configuration files however, are read if they are present in the location mentioned above. You therefore must ensure that every time you use the `start` command, the number of entries in configuration files are equal to the number of instances in their respective security group.

### Adding servers to the cluster

This is not yet fully supported using the script. You may have to manually launch an instance with `(cluster-name)-stores` group, and then use launch command with the `--resume` option.

### Listing members of the cluster

**To get the first locator's hostname:**
```no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem get-locator cluster-name
```
Use the `get-lead` command to get the first lead's hostname.

### Connecting to the cluster

You can connect to any instance of a cluster with SSH using the login command. It logs you into the first lead instance. You can then use SSH to connect to any other member of the cluster without a password. </br>
The SnappyData product directory is located under **/home/ec2-user/snappydata/** on all the members.
```no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem login cluster-name
```

### Destroying the cluster

Destroying a cluster permanently destroys all the data on the local instance stores and on the attached EBS volumes.
```no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem destroy cluster-name
```
This retains the security groups created for this cluster. To delete this, use it with `--delete-` group option.

### Starting cluster with Apache Zeppelin

Optionally, you can start an instance of Apache Zeppelin server with the cluster. [Apache Zeppelin](https://zeppelin.apache.org/) is a web-based notebook that enables interactive notebook. The Zeppelin server is launched on the same EC2 instance where the lead node is running.

```no-highlight
./snappy-ec2 -k my-ec2-key -i ~/my-ec2-key.pem --with-zeppelin launch cluster-name
```

### More options

For a complete list of options provided by the script run `./snappy-ec2` (available in the  snappydata-ec2-<version>.tar.gz compressed file). </br>The options are also provided below for quick reference.

```no-highlight
Usage: snappy-ec2 [options] <action> <cluster_name>

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
                        Name of the key pair to use on instances
  -i IDENTITY_FILE, --identity-file=IDENTITY_FILE
                        SSH private key file to use for logging into instances
  -p PROFILE, --profile=PROFILE
                        If you have multiple profiles (AWS or boto config),
                        you can configure additional, named profiles by using
                        this option (default: none)
  -t INSTANCE_TYPE, --instance-type=INSTANCE_TYPE
                        Type of instance to launch (default: m4.large).
                        WARNING: must be 64-bit; small instances won't work
  --locator-instance-type=LOCATOR_INSTANCE_TYPE
                        Locator instance type (default: t2.medium)
  -r REGION, --region=REGION
                        EC2 region used to launch instances in, or to find
                        them in (default: us-east-1)
  -z ZONE, --zone=ZONE  Availability zone to launch instances in, or 'all' to
                        spread stores across multiple (an additional $0.01/Gb
                        for bandwidthbetween zones applies) (default: a single
                        zone chosen at random)
  -a AMI, --ami=AMI     Amazon Machine Image ID to use
  --enterprise          Use SnappyData Enterprise edition AMI from AWS
                        Marketplace to launch the cluster. Overrides --ami
                        option. Extra charges apply. (default: False)
  --with-zeppelin       Launch Apache Zeppelin server with the cluster. It'll
                        be launched on the same instance where lead node will
                        be running.
  --deploy-root-dir=DEPLOY_ROOT_DIR
                        A directory to copy into / on the first locator. Must
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
  --copy-aws-credentials
                        Add AWS credentials to hadoop configuration to allow
                        Snappy to access S3
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
```

## Known Limitations

* Launching the cluster on custom AMI (specified via `--ami` option) does not work if the user 'ec2-user' does not have sudo permissions.

* Support for option `--user` is incomplete.
