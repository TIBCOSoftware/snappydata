# Authentication - Connecting to a Secure Cluster

Authentication is the process of verifying someone's identity. When a user tries to log in, that request is forwarded to the specified LDAP directory to verify if the credentials are correct.
There are a few different ways to connect to a secure cluster using either JDBC (Thin) Client, Smart Connector Mode and Snappy Jobs. Accessing a secure cluster requires users to provide their user credentials.

## Using JDBC (Thin) Client

When using the JDBC client, provide the user credentials using connection properties 'user' and 'password'.

**Example: JDBC Client**
```pre
val props = new Properties()
props.setProperty("user", username);
props.setProperty("password", password);

val url: String = s"jdbc:snappydata://localhost:1527/"
val conn = DriverManager.getConnection(url, props)
```

**Example: Snappy shell**

```pre
connect client 'localhost:1527;user=user1;password=user123';
```
For more information, refer [How to connect using JDBC driver](/howto/connect_using_jdbc_driver.md).

## Using ODBC Driver

You can also connect to the TIBCO ComputeDB cluster using TIBCO ComputeDB ODBC Driver using the following command:

```pre
Driver=SnappyData ODBC Driver;server=<ServerHost>;port=<ServerPort>;user=<userName>;password=<password>
```

For more information refer to, [How to Connect using ODBC Driver](../howto/connect_using_odbc_driver.md).

## Using Smart Connector Mode 

In Smart Connector mode, provide the user credentials as Spark configuration properties named `spark.snappydata.store.user` and `spark.snappydata.store.password`.

**Example**</br> 
In the below example, these properties are set in the `SparkConf` which is used to create `SnappyContext` in your job.

```pre
val conf = new SparkConf()
    .setAppName("My Spark Application with SnappyData")
    .setMaster(s"spark://$hostName:7077")
    .set("spark.executor.cores", TestUtils.defaultCores.toString)
    .set("spark.executor.extraClassPath",
      getEnvironmentVariable("SNAPPY_HOME") + "/jars/*" )
    .set("snappydata.connection", snappydataLocatorURL)
    .set("spark.snappydata.store.user", username)
    .set("spark.snappydata.store.password", password)
val sc = SparkContext.getOrCreate(conf)
val snc = SnappyContext(sc)
```

**Example**</br> 
The below example demonstrates how to connect to the cluster via Spark shell using the `--conf` option to specify the properties.

```pre
$./bin/spark-shell  
    --master local[*] 
    --conf spark.snappydata.connection=localhost:1527 
    --conf spark.snappydata.store.user=user1
    --conf spark.snappydata.store.password=user123
```

**Example**:</br>
Alternatively, you can specify the user credentials in the Spark conf file. </br> Spark reads these properties when you lauch the spark-shell or invoke spark-submit. 
To do so, specify the user credentials in the **spark-defaults.conf** file, located in the **conf** directory.

In this file, you can specify:
``` pre
spark.snappydata.store.user     <username>
spark.snappydata.store.password <password>
```

## Using Snappy Jobs

When submitting Snappy jobs using `snappy-job.sh`, provide user credentials through a configuration file using the option `--passfile`. 

For example, a sample configuration file is provided below: 

```pre
$ cat /home/user1/snappy/job.config 
-u user1:password
```

In the below example, the above configuration file is passed when submitting a job.
```pre
$./bin/snappy-job.sh submit  \
    --lead localhost:8090  \
    --app-name airlineApp \
    --class  io.snappydata.examples.CreateAndLoadAirlineDataJob \
    --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar \
    --passfile /home/user1/snappy/job.config
```
!!! Note

	* When checking the status of a job using `snappyjob.sh status --jobid`, provide user credentials through a configuration file using the option `--passfile`

	* Only trusted users should be allowed to submit jobs, as an untrusted user may be able to do harm through jobs by invoking internal APIs which can bypass the authorization checks. 
	
	* Currently, SparkJobServer UI may not be accessible when security is enabled, but you can use the `snappy-job.sh` script to access any information required using commands like `status`, `listcontexts`, etc. </br> Execute `./bin/snappy-job.sh` for more details.

	* The configuration file should be in a secure location with read access only to an authorized user.

	* These user credentials are passed to the Snappy instance available in the job, and it will be used to authorize the operations in the job.


