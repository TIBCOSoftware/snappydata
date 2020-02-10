# Executing Spark Scala Code using SQL  

!!!Note
	This is an experimental feature in the TIBCO ComputeDB 1.2.0 release.

Prior to the 1.2.0 release, any execution of a Spark scala program required the user to compile his Spark program, comply to specific callback API required by ComputeDB, package the classes into a JAR, and then submit the application using **snappy-job** tool. While, this procedure may still be the right option for a production application, it is quite cumbersome for the developer or data scientist wanting to quickly run some Spark code within the TIBCO ComputeDB store cluster and iterate. 

With the introduction of the [**exec scala**](/reference/sql_reference/exec-scala.md) SQL command, you can now get a JDBC or ODBC connection to the cluster and submit ad-hoc scala code for execution. The JDBC connection provides an ongoing session with the cluster so applications can maintain state and use/return this across multiple invocations.

Beyond this developer productivity appeal, this feature also allows you to skip using the [**Smart Connector**](/affinity_modes/connector_mode.md) in several cases.
Bespoke Spark applications using the smart connector are required to launch a client Spark application with its own executors and the store cluster is mostly providing parallel access to managed data. As the client is running queries this is additional capacity that you must budget. Moreover, the overall architecture is tough to understand. 
With [**exec scala**](/reference/sql_reference/exec-scala.md), any Spark application can submit scala code as an SQL command now. 

**Here is a use case that motivated us**:  TIBCO data scientists using Team Studio (TIBCO Data Science platform) can now build custom operators, target TIBCO ComputeDB for in-database computations, and run adhoc Spark Scala code. For instance, ETL feature engineering using Spark ML or running a training job in-memory and with parallelism. 
Refer to [exec scala usage examples](/reference/sql_reference/exec-scala.md#examplesofexec)

Moreover, [snappy-scala CLI](/reference/command_line_utilities/scala-cli.md) is provided which is built on top of the [**exec scala**](/reference/sql_reference/exec-scala.md) feature and the already existing snappy CLI utility. This connects to the TIBCO ComputeDB cluster using the JDBC driver. [snappy-scala CLI](/reference/command_line_utilities/scala-cli.md) is not a true scala interpreter but mimics a scala or a Spark shell type of interpreter. 