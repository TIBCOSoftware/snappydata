
## Table of Contents
* [Introduction](#introduction)
* [Download](#download-binary-distribution)
* [Community Support](#community-support)
* [Link with SnappyData Distribution](#link-with-snappydata-distribution)
* [Building SnappyData from Source](#building-snappydata-from-source)
* [Setting up passwordless SSH](#setting-up-passwordless-SSH)
* [Quick Start with SQL](#quick-start-with-sql)
* [Quick Start with Scala/Spark/Snappy Programming](#quick-start-with-scalasparksnappy-programming)

## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP (online transaction processing) and OLAP (online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD (as an in-memory transactional store with scale-out SQL semantics). 

![SnappyDataOverview](https://prismic-io.s3.amazonaws.com/snappyblog/c6658eccdaf158546930376296cd7c3d33cff544_jags_resize.png)

## Download binary distribution
You can download the latest version of SnappyData here:

* SnappyData Preview 0.2.1 download link [(tar.gz)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.2.1-preview/snappydata-0.2.1-PREVIEW-bin.tar.gz) [(zip)](https://github.com/SnappyDataInc/snappydata/releases/download/v0.2.1-preview/snappydata-0.2.1-PREVIEW-bin.zip)

SnappyData has been tested on Linux and Mac OSX. If not already installed, you will need to download [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). 

## Community Support

We monitor channels listed below for comments/questions.

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [IRC](http://webchat.freenode.net/?randomnick=1&channels=%23snappydata&uio=d4) ![IRC](http://i.imgur.com/vbH3Zdx.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          JIRA (coming soon) ![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData distribution
SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:
```
groupId: io.snappydata
artifactId: snappy-core_2.10
version: 0.2.1-PREVIEW

groupId: io.snappydata
artifactId: snappy-tools_2.10
version: 0.2.1-PREVIEW
```

If you are using sbt, add this line to your build.sbt for core snappy artifacts:

`libraryDependencies += "io.snappydata" % "snappy-core_2.10" % "0.2.1-PREVIEW"`

For additions related to snappy-store (column and row store etc), use:

`libraryDependencies += "io.snappydata" % "snappy-tools_2.10" % "0.2.1-PREVIEW"`

Check out more specific SnappyData artifacts here: http://mvnrepository.com/artifact/io.snappydata

## Working with SnappyData Source Code
If you are interested in working with the latest code or contributing to SnappyData development, you can also check out the master branch from Git:
```
Master development branch
git clone https://github.com/SnappyDataInc/snappydata.git --recursive

###### 0.x preview release branch with stability and other fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.x-preview --recursive
```

#### Building SnappyData from source
You will find the instructions for building, layout of the code, integration with IDEs using Gradle, etc here:

* [SnappyData Build Instructions](docs/build-instructions.md)

>  NOTE:
> SnappyData is built using Spark 1.6 (build xx) which is packaged as part of SnappyData. While you can build your application using Apache Spark 1.5, you will need to link to Snappy-spark to make  use of the SnappyData extensions. Gradle build tasks are packaged.  

You can try our quick starts or go directly to Getting Started to understand some of the concepts and features as you try out the product. 

* [Getting Started](docs/GettingStarted.md)

#### Setting up passwordless SSH

The quick start scripts use ssh to start up various processes. You can install ssh on ubuntu with `sudo apt-get install ssh`. ssh comes packaged with Mac OSX, however, make sure ssh is enabled by going to `System Preferences -> Sharing` and enabling `Remote Login`. Enabling passwordless ssh is the easiest way to work with SnappyData and prevents you from having to put in your ssh password multiple times. 

Generate an RSA key with 

`ssh-keygen -t rsa`

This will spawn some prompts. If you want truly passwordless ssh, just press enter instead of entering a password.

Finally, copy your key to authorized keys:

`cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`

More detail on passwordless ssh can be found [here](https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys--2) and [here](http://stackoverflow.com/questions/7134535/setup-passphraseless-ssh-to-localhost-on-os-x).

## Quick start with SQL  

This 5 minute tutorial provides a quick introduction to SnappyData. It exposes you to the cluster runtime and running OLAP and OLTP SQL.

The following script starts up a minimal set of essential components to form a SnappyData cluster - A locator, one data server 
and one lead node. All nodes are started on localhost.
The locator is primarily responsible for making all the nodes aware of each other, it allows the cluster to expand or shrink dynamically and provides a consistent membership view to each node even in the presence of failures (a distributed system membership service). The Lead node hosts the Spark Context and driver and orchestrates execution of Spark Jobs. 
The Data server is the work horse - manages all in-memory data, the OLTP execution engine and Spark executors. 

See the  [‘Getting Started’](docs/GettingStarted.md) section for more details. 

From the product install directory run this script:

````shell
./sbin/snappy-start-all.sh
````
This may take 30 seconds or more to bootstrap the entire cluster on your local machine (logs are in the 'work' sub-directory). 
The output should look something like this …
````
$ sbin/snappy-start-all.sh 
localhost: Starting SnappyData Locator using peer discovery on: 0.0.0.0[10334]
...
localhost: SnappyData Locator pid: 56703 status: running

localhost: Starting SnappyData Server using locators for peer discovery: jramnara-mbpro[10334]   
       (port used for members to form a p2p cluster)
localhost: SnappyData Server pid: 56819 status: running
localhost:   Distributed system now has 2 members.

localhost: Starting SnappyData Leader using locators for peer discovery: jramnara-mbpro[10334]
localhost: SnappyData Leader pid: 56932 status: running
localhost:   Distributed system now has 3 members.

localhost:   Other members: jramnara-mbpro(56703:locator)<v0>:54414, jramnara-mbpro(56819:datastore)<v1>:39737

````
At this point, the SnappyData cluster is up and running and is ready to accept jobs and SQL requests via JDBC/ODBC.
You can [monitor the Spark cluster at port 4040](http://localhost:4040).

For SQL, the SnappyData SQL Shell `snappy-shell` provides a simple way to inspect the catalog,  run admin operations, 
manage the schema and run interactive queries. 

From product install directory run: 
````
$ ./bin/snappy-shell
````
Now, you are ready to try connecting and running SQL on SnappyData. 
On the `snappy-shell` prompt:

Connect to the cluster with

````snappy> connect client 'localhost:1527';````

And check member status with:

````snappy> show members;````

Now, lets create one column and one row table and load some data. Simply copy/paste to the shell. 
```sql
snappy> run './quickstart/scripts/create_and_load_column_table.sql';
snappy> run './quickstart/scripts/create_and_load_row_table.sql';
```

Now, you can run analytical queries or execute execute transactions on this data. OLAP queries are automatically executed 
through Spark driver and executors. 

```sql
snappy> run './quickstart/scripts/olap_queries.sql';
```

You can study the memory consumption, query execution plan, etc. from the [Spark console](http://localhost:4040).

Congratulations! 

## Where to go next?

Next, we recommend going through more in-depth examples in [Getting Started](docs/GettingStarted.md). Here you will find more details on the 
concepts and experience SnappyData’s AQP, Stream analytics functionality both using SQL and Spark API.
You can also go through our very preliminary [docs](http://snappydatainc.github.io/snappydata/) and provide us your comments. 

If you're interested in the Scala/Spark side of things, go through the [programming quick start below](#quick-start-with-scalasparksnappy-programming).

If you are interested in contributing please visit the [contributor page](http://www.snappydata.io/community/contributors) for ways in which you can help. 

## Quick start with Scala/Spark/Snappy Programming

Click the screenshot below to watch the screencast that goes along with this section:
[![Screencast](http://i.imgur.com/ZbMltwl.png)](https://www.youtube.com/watch?v=S297Wd-2UTs)

SnappyData provides the same [Spark/Scala REPL session](http://spark.apache.org/docs/latest/quick-start.html) that any Spark user is familiar with. You simply start it with a special configuration to have access to SnappyData extensions. Remember as you follow this guide that paste mode can always be entered in the REPL using `:paste` and you exit paste mode with `ctrl+d`.

First, make sure you have started the SnappyData servers as described [above](https://github.com/SnappyDataInc/snappydata/blob/master/README.md#quick-start-with-sql) (entered from the root directory):

````
./sbin/snappy-start-all.sh
````

To start the Spark/Scala REPL session enter the following command from the root, /snappy/ directory:

````
./bin/spark-shell  --master local[*] --conf snappydata.store.locators=localhost:10334 --conf spark.ui.port=4041
````

From here, all the classic [Spark transformations](http://spark.apache.org/docs/latest/programming-guide.html#transformations) are possible. For example, the well-known as example from [Spark’s basic programming intro](http://spark.apache.org/docs/latest/quick-start.html#basics):

````scala
scala> val textFile = sc.textFile("RELEASE")
textFile: org.apache.spark.rdd.RDD[String]

scala> textFile.count()
res1: Long = 2

scala> val linesWithThree = textFile.filter(line => line.contains("3"))
linesWithThree: org.apache.spark.rdd.RDD[String]

scala> linesWithThree.collect()
res14: Array[String] = Array(Snappy Spark 0.1.0-SNAPSHOT 3a85dca6b4e039dd5a1be43f1f52bcb2034bfc03 built for Hadoop 2.4.1)
````
**But what about SnappyData extensions?** To use SnappyData extensions, we must either import or create a new SnappyContext object, which gets passed the existing SparkContext (sc):

````scala
val snc = org.apache.spark.sql.SnappyContext(sc)
````
Let’s create a new column table (the table optimized for OLAP querying):
````scala
case class Data(COL1: Int, COL2: Int, COL3: Int)
val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))

val dataDF = snc.createDataFrame(rdd)
````
Here we’ve defined some data which we’ve placed into a case class and parallelized into rdd’s. classic Spark programming. Next, we create a DataFrame, but we use the previously defined SnappyContext, this allows us to use all the SnappyData extensions on the DataFrame.

Now, let’s create a column table using what we’ve already defined:

````scala
val props1 = Map("Buckets" -> "2")
snc.createTable("COLUMN_TABLE", "column", dataDF.schema, props1)
````
`props1` allows us to define the optional `“Buckets”` attribute which specifies the smallest unit that can be moved around in the SnappyStore when data migrates. Within `createTable`, we’ve defined the table’s name, the type of table, the table’s schema, and provided the Buckets information contained in `props1`.

Now, let’s insert the data in append-mode:

````scala
dataDF.write.format("column").mode(org.apache.spark.sql.SaveMode.Append)
  .options(props1).saveAsTable("COLUMN_TABLE")
```

Here we’ve written the data contained in dataDF to our newly created column table using Append mode. Let’s print the table using SQL and see what’s inside:

````scala
val results1 = snc.sql("SELECT * FROM COLUMN_TABLE")
  results1.foreach(println)
````

Easy enough. But how do we create a **row table** out of the same data, i.e. a table that can be **mutated and updated**?

First, let’s create the actual table using the `createTable` method. `”Buckets”` is not used this time.

````scala
val props2 = Map.empty[String, String]
  snc.createTable("ROW_TABLE", "row", dataDF.schema, props2)
````

Let’s insert the dataDF data as we did before, in append mode:

````scala
dataDF.write.format("row").mode(org.apache.spark.sql.SaveMode.Append)
  .options(props2).saveAsTable("ROW_TABLE")
````

Now, let’s check our results before we mutate some data to compare the difference:

````scala
 val results2 = snc.sql("SELECT * from ROW_TABLE")
  results2.foreach(println)
````

Okay, there’s our row table. Now let’s do some mutation:

````scala
  snc.update("ROW_TABLE", "COL3 = 3", org.apache.spark.sql.Row(99), "COL3" )
````

Here we’re updating all the values in ROW_TABLE in column 3 that equal 3 to the value 99. Let’s print our mutated table and make sure it worked:

````scala
val results3 = snc.sql("SELECT * FROM ROW_TABLE")
 results3.foreach(println)
````

And voila! Mutations in Spark. This is a very simple, abbreviated example of what SnappyData can do. It becomes much more interesting when working on streaming data, joining streams with reference data, using approximate query processing and more. To learn more about these advanced use cases, check out our [Getting Started with the Spark API](https://github.com/SnappyDataInc/snappydata/blob/master/docs/GettingStarted.md#getting-started-with-spark-api) and [Developing apps using the Spark API](http://snappydatainc.github.io/snappydata/jobs/). To read more specifically about the SnappyContext check out our [SnappyContext Documentation](http://snappydatainc.github.io/snappydata/jobs/#snappycontext).

