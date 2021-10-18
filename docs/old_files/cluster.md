SnappyData, a database server cluster, has three main components - Locator, Server and Lead.

- **Locator**: Provides discovery service for the cluster. Informs a new member joining the group about other existing members. A cluster usually has more than one locator for high availability reasons.
- **Lead Node**: Acts as a Spark driver by maintaining a singleton SparkContext. There is one primary lead node at any given instance but there can be multiple secondary lead node instances on standby for fault tolerance. The lead node hosts a REST server to accept and run applications. The lead node also executes SQL queries routed to it by “data server” members.
- **Data Servers**: Hosts data, embeds a Spark executor, and also contains a SQL engine capable of executing certain queries independently and more efficiently than Spark. Data servers use intelligent query routing to either execute the query directly on the node, or pass it to the lead node for execution by Spark SQL.

<p style="text-align: center;"><img alt="ClusterArchitecture" src="../GettingStarted_Architecture.png"></p>

For details of the architecture refer to [Architecture](../architecture.md)

SnappyData also has multiple deployment options. For more information refer to, [Deployment Options](../affinity_modes/index.md).

### Interacting with SnappyData

> Note: For the section on the Spark API, we assume some familiarity with [core Spark, Spark SQL and Spark Streaming concepts](http://spark.apache.org/docs/latest/).
And, you can try out the Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). All the commands and programs listed in the Spark guides work in SnappyData as well.
For the section on SQL, no Spark knowledge is necessary.

To interact with SnappyData, we provide interfaces for developers familiar with Spark programming as well as SQL. JDBC can be used to connect to the SnappyData cluster and interact using SQL. On the other hand, users comfortable with the Spark programming paradigm can write jobs to interact with SnappyData. Jobs can be like a self contained Spark application or can share state with other jobs using the SnappyData store.

Unlike Apache Spark, which is primarily a computational engine, the SnappyData cluster holds mutable database state in its JVMs and requires all submitted Spark jobs/queries to share the same state (of course, with schema isolation and security as expected in a database). This required extending Spark in two fundamental ways.

1. __Long running executors__: Executors are running within the SnappyData store JVMs and form a p2p cluster.  Unlike Spark, the application Job is decoupled from the executors - submission of a job does not trigger launching of new executors. 
2. __Driver runs in HA configuration__: Assignment of tasks to these executors are managed by the Spark Driver.  When a driver fails, this can result in the executors getting shutdown, taking down all cached state with it. Instead, we leverage the [Spark JobServer](https://github.com/spark-jobserver/spark-jobserver) to manage Jobs and queries within a "lead" node.  Multiple such leads can be started and provide HA (they automatically participate in the SnappyData cluster enabling HA). 
Read our [docs](../index.md) for details on the architecture.
 
In this document, we showcase mostly the same set of features via the Spark API or using SQL. If you are familiar with Scala and understand Spark concepts you may choose to skip the SQL part go directly to the [Spark API section](clustersparkapi.md).
