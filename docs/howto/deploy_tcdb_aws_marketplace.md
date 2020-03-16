# Deploying TIBCO ComputeDB from AWS Marketplace

You can quickly set up a simple TIBCO ComputeDB cluster on AWS using its Amazon Machine Image (AMI) that is available in the AWS Marketplace. The AMI comes with the sample CloudFormation templates.


!!!Note
	The TIBCO ComputeDB 1.2.0 AMI will be available shortly.

!!!Note
	The **IAM user** or the role with which you create the stack and other AWS resources as part of the stack must have adequate AWS permissions.

To deploy TIBCO ComputeDB from AWS Marketplace, do the following:

1.	Subscribe and launch the TIBCO ComputeDB offering in AWS Marketplace. You will be taken to the AWS CloudFormation’s **Create stack** page. The Amazon S3 URL of the template is already populated here.
2.	Click **Next**. The **Specify stack details** page is displayed.
4.	Enter the Stack name and the configuration details of the cluster using the following parameters and then click **Next**:

	| Parameters | Description |
	|--------|--------|
	|   **ClientCIDR**     |   The allowed CIDR IP range for client (JDBC/ODBC) connections. To access the cluster from your personal laptop, first, find your public IP address. You can go to [whatismyip.com](https://whatsmyip.com/) to get your IPv4 address and then append **/32** to it. For example, 123.201.112.0/32.     |
    |    **EBSVolumeSize**    |  Specify the Amazon EBS volume size (in GB), which will be attached to the EC2 instance that gets launched. It can be any number between 16 and 1024, depending on your data volume. The TIBCO ComputeDB catalog metadata and its database are managed in this volume. |
    |  **InstanceTypeName**      |   Select one instance type from the dropdown list as per your workload requirement. For more details, refer [https://aws.amazon.com/ec2/pricing/on-demand](https://aws.amazon.com/ec2/pricing/on-demand).|
    |   **KeyPairName**     |   Select one from the dropdown list whose key file (.pem) is available with you. This is needed to connect to the EC2 instance through SSH. |
    |  **VPCID** |   Select the DEFAULT VPC ID of this region from the dropdown list. Your instance is launched within this VPC. |
    |  **WebUICIDR**  |  The allowed CIDR IP range that can access TIBCO ComputeDB Monitoring Console. Can also be the same as ClientCIDR, for example, 123.201.112.0/32. To access the cluster from your personal laptop, first find your public IP address. You can go to [whatismyip.com](https://whatsmyip.com/) to get your IPv4 address and append **/32** to it.     |

	!!!Note
		On the next page, you may see the following error message if you do not have adequate permissions for AWS SNS topics:</br> `Failed to retrieve sns topics `</br>You can safely ignore this message and proceed to create the stack.

5.	In the **Configure stack options** page, you can configure advance options for the stack. Click **Next**. The **Review** page is displayed. 
6.	In the **Review** page, you can review and edit the stack details before proceeding to create the stack. This page also displays the stack description. The stack description mentions if the cluster will be launched with security enabled.
You can also view the details about the status of stack creation from the **Events** tab. The status changes to **CREATE_COMPLETE** when it is successfully completed.

	!!!Note
		A test LDAP server is launched within the launched EC2 instance to demonstrate a TIBCO ComputeDB cluster with security enabled. You can provision your LDAP server to work with the TIBCO ComputeDB cluster in production. For more details about configuring a secure ComputeDB cluster with LDAP, refer [Launching the Cluster in Secure Mode](../security/launching_the_cluster_in_secure_mode.md).

7.	After the **CREATE_COMPLETE** status is displayed, click the **Outputs** tab. The following two URLs are displayed:

	*	**ComuputeDBConsole**:</br>
		Click **ComputeDBConsole** URL to access the TIBCO ComputeDB monitoring console. The monitoring console  which provides an overview of the cluster is displayed. You can only access it from the machines whose IP falls in the CIDR IP range you provided as **WebUICIDR** while creating the stack.
	
    	!!!NOTE
			If your cluster is security enabled, you will be prompted for the username and password to access this console. Use **snappyuser** as the username and the EC2 instance ID as the password. You can find this instance ID on this page.
	
	*	**ZeppelinNotebooks**:</br>
		Click the **ZeppelinNotebooks** URL to access the Apache Zeppelin server that is launched along with the TIBCO ComputeDB cluster. You can only access it from the machines whose IP falls in the CIDR IP range you provided as **WebUICIDR** while creating the stack. It comes with a set of sample Notebooks for you to interact with the TIBCO ComputeDB cluster. Clone the Notebook you want to run. You can also create your own Notebooks. For more details, refer to [Using Apache Zepplin with TIBCO ComputeDB](/howto/use_apache_zeppelin_with_snappydata.md).

## Setting Credentials for SnappySession in Notebooks

If your cluster has security enabled, the credentials required for your Notebook to communicate with TIBCO ComputeDB cluster are already pre-configured in the interpreter settings. However, some of the Notebooks that use SnappySession APIs must be modified to set the credentials in the session's conf:

To modify SnappySession APIs in the Notebooks, do the following:

1.	From the **Notebook** dropdown, select **ComputeDB API Quickstart** Notebook.
	
    !!!Note
    	You must clone this notebook before you run its paragraphs. 

2.	Go to **Step 1**, where the SnappySession instance is created and add the following lines after the initialization:

	    	snappyNoteSession.conf.set(“user”, “snappyuser”)
    		snappyNoteSession.conf.set(“password”, “<EC2-Instance-ID>”)
	
    Refer to the following example:
   
            exec scala

            import snappy.implicits._
            val snappyNoteSession = new org.apache.spark.sql.SnappySession(sc)
            snappyNoteSession.conf.set(“user”, “snappyuser”)
            snappyNoteSession.conf.set(“password”, “<EC2-Instance-ID>”)
       
       Replace `<EC2-Instance-ID>` above with the actual instance ID.
    

## Connecting to the TIBCO ComputeDB Cluster from JDBC URL

You can also connect to the cluster via your JDBC program using the following JDBC connection string: 

```
jdbc:snappydata://ec2-instance-IP:1527/
```

!!!Note
	Your program (or JDBC client tool) should be running on a machine whose IP address is in the range you provided as **ClientCIDR** while creating the stack.</br>If your cluster has security enabled, you must also specify the username and password as connection properties.


## Connecting to the EC2 Instance via SSH

You can log into the EC2 instance via SSH from your laptop using the following command:

```
chmod 700 /path/to/your/key/file.pem
ssh -i /path/to/your/key/file.pem ec2-user@public-IP-ec2-instance
```

Your laptop's IP address must be in the range you provided as **ClientCIDR** while creating the stack. Your key file (**.pem**) should not have read/write permissions for other users.

TIBCO ComputeDB is installed at **/opt/tibco-computedb** and Apache Zeppelin is installed at **/opt/zeppelin**.
You can stop, configure, and start TIBCO ComputeDB or Zeppelin as required from these locations.

## Deleting the Stack

After you have finished using the clusters, you can delete the CloudFormation stack. 

On the AWS CloudFormation page, select the stack that you want to delete and click the **Delete** button located on the upper right side of the page. All the resources that were created as part of the stack creation will be deleted. 