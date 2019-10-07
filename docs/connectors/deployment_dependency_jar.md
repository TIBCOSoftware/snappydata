# Deploying Third Party Connectors

A job submitted to SnappyData, the creation of external tables as SQL or through API, the creation of a user-defined function etc. may have an external jar and package dependencies. For example, you may want to create an external table in SnappyData which points to a Cassandra table. For that, you would need the Spark Cassandra connector classes which are available in maven central repository.

Today, Spark connectors are available to virtually all modern data stores - RDBs, NoSQL, cloud databases like Redshift, Snowflake, S3, etc. Most of these connectors are available as mvn or spark packages or as published jars by the respective vendors. 
SnappyData’s compatibility with Spark allows SnappyData to work with the same connectors of all the popular data sources.

SnappyData’s **deploy** command allows you to deploy these packages by using its maven coordinates. When it is not available, you can simply upload jars into any provisioned SnappyData cluster.

SnappyData offers the following SQL commands:

*	**deploy package** - to deploy maven packages
*	**deploy jar** - to deploy your application or library Jars

Besides these SQL extensions, support is provided in SnappyData 1.1.1 version to deploy packages as part of SnappyData Job submission. This is similar to [Spark’s support](https://spark.apache.org/docs/latest/submitting-applications.html) for **--packages** when submitting Spark jobs.

The following sections are included in this topic:

*	[Deploying Packages in SnappyData](#deploypackages)
*	[Deploying Jars in SnappyData](#deployjars)
*	[Viewing the Deployed Jars and Packages](#listjarspackages)
*	[Removing Deployed Jars](#undeploy)
*	[Submitting SnappyData Job with Packages](#submitjobpackages)


<a id= deploypackages> </a>
## Deploying Packages in SnappyData

Packages can be deployed in SnappyData using the **DEPLOY PACKAGE** SQL. Execute the `deploy package` command to deploy packages. You can pass the following through this SQL:

*	Name of the package.
*	Repository where the package is located.
*	Path to a local cache of jars.

!!!Note
	SnappyData requires internet connectivity to connect to repositories which are hosted outside the network. Otherwise, the resolution of the package fails.

For resolving the package, Maven Central and Spark packages, located at [http://dl.bintray.com/spark-packages](http://dl.bintray.com/spark-packages), are searched by default. Hence, you must specify the repository only if the package is not there at **Maven Central** or in the **spark-package** repository.

!!!Tip
	Use **spark-packages.org** to search for Spark packages. Most of the popular Spark packages are listed here.

### SQL Syntax to Deploy Package Dependencies in SnappyData

```pre
deploy package <unique-alias-name> ‘packages’ [ repos ‘repositories’ ] [ path 'some path to cache resolved jars' ]
```
*	**unique-alias-name** - A name to identify a package. This name can be used to remove the package from the cluster.  You can use alphabets, numbers, and underscores to create the name.

*	**packages** - Comma-delimited string of maven packages. 

*	**repos** - Comma-delimited string of remote repositories other than the **Maven Central** and **spark-package** repositories. These two repositories are searched by default.  The format specified by Maven is: **groupId:artifactId:version**.

*	**path** - The path to the local repository. This path should be visible to all the lead nodes of the system. If this path is not specified then the system uses the path set in the **ivy.home** property. if even that is not specified, the **.ivy2** in the user’s home directory is used.

### Example 

**Deploy packages from a default repository:**
	
``` pre
deploy package deeplearning 'databricks:spark-deep-learning:0.3.0-spark2.2-s_2.11' path '/home/snappydata/work';
```

```pre
deploy package Sparkredshift 'com.databricks:spark-redshift_2.10:3.0.0-preview1' path '/home/snappydata/work';
```

<a id= deployjars> </a>
## Deploying Jars in SnappyData

SnappyData provides a method to deploy a jar in a running system through **DEPLOY JAR** SQL. You can execute the `deploy jar` command to deploy dependency jars. 

### Syntax for Deploying Jars in SnappyData
```pre
deploy jar <unique-alias-name> ‘jars’
```
*	**unique-alias-name** - A name to identify the jar. This name can be used to remove the jar from the cluster.  You can use alphabets, numbers and underscores to create the name.

*	**jars** - Comma-delimited string of jar paths. These paths are expected to be accessible from all the lead nodes in SnappyData.

### Example 

**Deploying jars:**

```
deploy jar SparkDaria spark-daria_2.11.8-2.2.0_0.10.0.jar
```

All the deployed commands are stored in the SnappyData cluster. In cases where the artifacts of the dependencies are not available in the provided cache path, then during restart, it automatically resolves all the packages and jars again and installs them in the system.


<a id= listjarspackages> </a>
## Viewing the Deployed Jars and Packages

You can view all the packages and jars deployed in the system by using the `list packages` command. 

### Syntax for Listing Deployed Jars and Packages

```pre
snappy> list packages;
Or
snappy> list jars;
```

Both of the above commands will list all the packages as well the jars that are installed in the system. Hence, you can use either one of those commands.

### Sample Output of Listing Jars/Packages


```pre
snappy> list jars;
alias          |coordinate                                     |isPackage
-------------------------------------------------------------------------
CASSCONN       |datastax:spark-cassandra-connector:2.0.1-s_2.11|true     
MYJAR          |b.jar                                          |false 

```
<a id= undeploy> </a>
## Removing Deployed Jars

You can remove the deployed jars with the **undeploy** command. This command removes the jars that are directly installed and the jars that are associated with a package, from the system.

### Syntax for Removing Deployed Jars

```pre
undeploy <unique-alias-name>;
```

!!!Note
	The removal is only captured when you use the **undeploy** command, the jars are removed only when you restart the cluster.

### Example 
	
```pre
undeploy spark_deep_learning_0_3_0;
```

!!!Attention
	*	If during restart, for any reason the deployed jars and packages are not reinstalled automatically by the system, a warning is shown in the log file. If you want to fail the restart, then you need to set a system property in the **conf** file to stop restarts completely. The name of the system property is **FAIL_ON_JAR_UNAVAILABILITY**.
	*	If you want to use external repositories, ensure to maintain internet connectivity at least on the lead nodes.
	*	It is highly recommended to use a local cache path to store the downloaded jars of a package, because the next time the same deploy is executed, it can be picked from the local path.
	*	Ensure that this path is available on the lead nodes.
	*	Similarly keep the standalone jars also in a path where it is available to all the lead nodes.

<a id= submitjobpackages> </a>
## Submitting SnappyData Job with Packages

You can specify the name of the packages which can be used by a job that is submitted to SnappyData. This package is visible only to the job submitted with this argument. If another job tries to access a class belonging to the jar of this package then it will get ClassNotFoundException.

A **--packages** option is added to the **snappy-job.sh** script where you can specify the packages. Use the following syntax:

```pre
$./snappy-job.sh submit --app-name <app-name> --class <job-class> [--lead <hostname:port>]  [--app-jar <jar-path>] [other existing options]       [--packages <comma separated package-coordinates> ] [--repos <comma separated mvn repositories] [--jarcache <path where resolved jars will be kept>]
```

### Example of SnappyData Job Submission with Packages

```pre
./bin/snappy-job.sh submit --app-name cassconn --class <SnappyJobClassName> --app-jar <app.jar> --lead localhost:8090 --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.7
```

If you want global deployment, you can instead use the [**deploy**](#deployjars) command SQL and then the packages become visible everywhere.

```pre
snappy> deploy package cassconn 'datastax:spark-cassandra-connector:2.0.1-s_2.11' path '/home/alex/mycache';
```


