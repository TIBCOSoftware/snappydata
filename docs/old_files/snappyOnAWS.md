````
The ec2 script is still under development. Feel free to try it out and provide your feedback.
````
## Introduction

The `snappy-ec2` script, present in SnappyData's ec2 directory, enables users to quickly launch and manage SnappyData clusters on Amazon EC2. Users can also configure the individual nodes of the cluster by providing properties in specific conf files which the script would read before launching the cluster.

The `snappy-ec2` script has been derived from the `spark-ec2` script available in [Apache Spark 1.6](https://github.com/apache/spark/tree/branch-1.6/ec2). 

**Prerequisites**

You need to have an account with [Amazon Web Service (AWS)](http://aws.amazon.com/) with adequate credentials to launch EC2 resources.
You also need to set two environment variables viz. AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID with your AWS secrete access key and ID. These can be obtained from the [AWS homepage](http://aws.amazon.com/) by clicking Account > Security Credentials > Access Credentials.
````
export AWS_SECRET_ACCESS_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112
export AWS_ACCESS_KEY_ID=A1B2C3D4E5F6G7H8I9J10
````

Alternatively, if you already have setup AWS Command Line Interface on your local machine, these credentials should be picked up from the aws credentials file by the script.


## Cluster management

**Launching a cluster**

You can launch and manage multiple clusters using this script and each of the clusters is identified by the unique cluster name you provide. The script internally ties members (locators, leads and stores) of the cluster with [EC2 security groups](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html) whose names are derived from the provided cluster name.

So if you launch a cluster named 'my-cluster', it would have its locator in security group named 'my-cluster-locator' and its stores in 'my-cluster-stores'. 

By default, the script starts one instance of locator, lead and server each. Below command will start a snappydata cluster named 'snappydata-cluster' with 4 stores (or servers) in N. Virginia region of AWS.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file --stores=4 --region=us-east-1 launch snappydata-cluster
````

**Specifying properties**

If you want to configure each of the locator, lead or servers with specific properties, you can do so by specifying them in files named locators, leads or servers, respectively and placing these under product_dir/ec2/deploy/home/ec2-user/snappydata/. Refer to [this SnappyData documentation page](http://snappydatainc.github.io/snappydata/configuration/#configuration-files) for example on how to write these conf files.

This is similar to how one would provide properties to snappydata cluster nodes while launching it via `sbin/snappy-start-all.sh` script.

The only and important difference here is that, instead of the hostnames of the locator, lead or store, you will have to write {{LOCATOR_N}}, {{LEAD_N}} or {{SERVER_N}} in these files, respectively. N stands for Nth locator, lead or server. The script will replace these with the actual hostname of the members when they are launched.

The sample conf files for a cluster with 2 locators, 1 lead and 2 stores are given below.

*locators*
````
{{LOCATOR_0}} -peer-discovery-port=9999 -heap-size=1024m
{{LOCATOR_1}} -peer-discovery-port=9888 -heap-size=1024m 
````
*leads*
````
{{LEAD_0}} -heap-size=4096m -spark.ui.port=3333 -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -spark.executor.cores=10
````
*servers*
````
{{SERVER_0}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888
{{SERVER_1}} -heap-size=4096m -locators={{LOCATOR_0}}:9999,{{LOCATOR_1}}:9888 -client-port=1530
````
When you run snappy-ec2, it'll look for these files under ec2/deploy/home/ec2-user/snappydata/ and, if present, read them while launching the cluster on Amazon EC2. You must ensure that the number of locators, leads or servers specified by options --locators, --leads or --stores must match to the number of entries in their respective conf files.

The script will also read snappy-env.sh, if present in this location.

**Stopping a cluster**

When you stop a cluster, it will shut down the EC2 instances and you will loose any data saved on its local instance stores but the data saved on EBS volumes, if any, will be retained unless the spot-instances were used.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file stop cluster-name
````

**Starting a cluster**

When you start a cluster, it uses the existing EC2 instances associated with the cluster name and launches SnappyData processes on them.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file start cluster-name
````
Note that the start command (or launch command with --resume option) ignores --locators, --leads or --stores options and launches SnappyData cluster on existing instances. But the conf files will be read in any case, if they are present in the location mentioned above. So you need to ensure that every time you use start command, the number of entries in conf files are equal to the number of instances in their respective security group.  

**Adding servers to  a cluster**

This is not yet fully supported via the script. You may have to manually launch an instance with `(cluster-name)-stores` group and then use launch command with --resume option.

**Listing members of a cluster**

To get the first locator's hostname,
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file get-locator cluster-name
````
To get the first lead's hostname, use get-lead command.

**Connecting to a cluster**

You can connect to any instance of a cluster via ssh using the login command. It'll log you into the first lead instance. From there, you can ssh to any other member of the cluster without password.
The SnappyData product directory would be located under /home/ec2-user/snappydata/ on all the members.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file login cluster-name
````

**Destroying a cluster**

Destroying a cluster will destroy all the data on the local instance stores as well as on the attached EBS volumes permanently.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file destroy cluster-name
````
This will retain the security groups created for this cluster. To delete them as well, use it with --delete-group option.

**Starting cluster with Apache Zeppelin**

Optionally, you can start an instance of Apache Zeppelin server with the cluster. [Apache Zeppelin](https://zeppelin.apache.org/) is a web-based notebook that enables interactive notebook. The Zeppelin server is launched on the same EC2 instance where the lead node is running.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file --with-zeppelin launch cluster-name
````

**More options**

For a complete list of options this script has, execute `./snappy-ec2`. These are pasted below for your quick reference.
````
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
  --with-zeppelin
                        Launch Apache Zeppelin server with the cluster. It'll
                        be launched on the same instance where lead node will
                        be running.
  --deploy-root-dir=DEPLOY_ROOT_DIR
                        A directory to copy into / on the first master. Must
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


## Limitations

1. Launching cluster on custom AMI (specified via --ami option) will not work if it does not have the user 'ec2-user' with sudo permissions.
2. Support for option --user is incomplete.
