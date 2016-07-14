````
The ec2 script is still under development and some of the features may be incomplete.
````
## Introduction

The `snappy-ec2` script, present in SnappyData's ec2 directory, enables users to quickly launch and manage SnappyData clusters on Amazon EC2. Users can also configure the nodes of the cluster by providing properties in specific conf files which the script would read before launching the cluster.

The `snappy-ec2` script has largely been derived from the `spark-ec2` script available in [Apache Spark 1.6](https://github.com/apache/spark/tree/branch-1.6/ec2). 

**Prerequisites**

You need to have an account with [Amazon Web Service (AWS)](http://aws.amazon.com/) with adequate credentials to launch EC2 resources.
You also need to set two environment variables viz. AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID with your AWS secrete access key and ID. These can be obtained from the [AWS homepage](http://aws.amazon.com/) by clicking Account > Security Credentials > Access Credentials.
````
export AWS_SECRET_ACCESS_KEY=abcD12efGH34ijkL56mnoP78qrsT910uvwXYZ1112
export AWS_ACCESS_KEY_ID=A1B2C3D4E5F6G7H8I9J10
````

Alternatively, if you have already setup AWS Command Line Interface on your local machine, these credentials should be picked up from the aws config file by the script.


## Cluster management

**Launching a cluster**

You can launch and manage multiple clusters and each of the clusters are identified by the unique cluster name you provide. The script internally ties members (locators, leads and stores) of the cluster with [EC2 security groups](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html) whose names are derived from the provided cluster name. So if you launch a cluster named 'my-cluster', it would have its locator in security group named 'my-cluster-locator' and its stores in 'my-cluster-stores'. 

Below command will start a snappydata cluster named 'snappydata-cluster' with 4 stores (or servers) in N. Virginia region of AWS.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file --stores 4 --region us-east-1 launch snappydata-cluster
````

**Specifying properties**

If you want to configure each of the locator, lead or servers with specific properties, you can do so by specifying them in files named locators, leads or servers, respectively and placing these under product_dir/ec2/deploy/home/ec2-user/snappydata/. Refer to [this SnappyData documentation page](http://snappydatainc.github.io/snappydata/configuration/#configuration-files) for example on how to write these conf files.

The only and important difference here is that, instead of the hostname of the locator, lead or store, you will have to write {{LOCATOR_N}}, {{LEAD_N}} or {{SERVER_N}} in these files, respectively. N stands for Nth locator, lead or server. The script will replace these with the actual hostname of the members when they are launched. A typical conf/servers for a cluster with 2 servers would look something like
````
{{SERVER_0}} -heap-size=4096m -locators={{LOCATOR_0}}:9999
{{SERVER_1}} -heap-size=4096m -locators={{LOCATOR_0}}:9999
````
So when you run snappy-ec2 next time, it'll read these conf files while launching the cluster on Amazon EC2. The script will also read snappy-env.sh, if present in this location.

This is similar to how one would provide properties to snappydata cluster nodes while launching it via `sbin/snappy-start-all.sh` script.

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

**Adding servers to  a cluster**

This is not yet fully supported via the script. You may have to manually launch an instance with `(cluster-name)-stores` group and then use launch command with --resume option.

**Listing members of a cluster**

To get the locator's hostname,
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file get-locator cluster-name
````
To get the lead's hostname, use get-lead command.

**Destroying a cluster**

Destroying a cluster will destroy all the data on the local instance stores as well as on the attached EBS volumes permanently.
````
./snappy-ec2 -k ec2-keypair-name -i /path/to/keypair/private/key/file destroy cluster-name
````
This will retain the security groups created for this cluster. To delete them as well, use it with --delete-group option.




