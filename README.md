
## Introduction
SnappyData is a **distributed in-memory data store for real-time operational analytics, delivering stream analytics, OLTP (online transaction processing) and OLAP (online analytical processing) in a single integrated cluster**. We realize this platform through a seamless integration of Apache Spark (as a big data computational engine) with GemFire XD (as an in-memory transactional store with scale-out SQL semantics). 

![SnappyDataOverview](https://prismic-io.s3.amazonaws.com/snappyblog/c6658eccdaf158546930376296cd7c3d33cff544_jags_resize.png)

## Download binary distribution
You can download the latest version of SnappyData here:

* [SnappyData Preview 0.1 download link](1)

SnappyData has been tested on Linux and Mac OSX. If not already installed, you will need to download [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). 

## Community Support

We monitor channels listed below for comments/questions.

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [IRC](http://webchat.freenode.net/?randomnick=1&channels=%23snappydata&uio=d4) ![IRC](http://i.imgur.com/vbH3Zdx.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          JIRA (coming soon) ![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData distribution
SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:
```
groupId: io.snappydata
artifactId: snappydata_2.10
version: 0.1_preview
```

## Working with SnappyData Source Code
If you are interested in working with the latest code or contributing to SnappyData development, you can also check out the master branch from Git:
```
Master development branch
git clone https://github.com/SnappyDataInc/snappydata.git

###### 0.1 preview release branch with stability fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b 0.1_preview
```

#### Building SnappyData from source
You will find the instructions for building, layout of the code, integration with IDEs using Gradle, etc here:

* [SnappyData Build Instructions](featureDocs/build-instructions.md)

>  NOTE:
> SnappyData is built using Spark 1.6 (build xx) which is packaged as part of SnappyData. While you can build your application using Apache Spark 1.5, you will need to link to Snappy-spark to make  use of the SnappyData extensions. Gradle build tasks are packaged.  

You can try our quick start or keep reading to understand some of the concepts and features as you try out the product. 

## Quick start  

This 5 minute tutorial provides a quick introduction to SnappyData. It exposes you to the cluster runtime and running OLAP, OLTP SQL.

The following script starts up a minimal set of essential components to form a SnappyData cluster - A locator, one data server 
and one lead node. All nodes are started on localhost.
The locator is primarily responsible to make all the nodes aware of each other, allows the cluster to expand or shrink dynamically and provides a consistent membership view to each node even in the presence of failures (a distributed system membership service). The Lead node hosts the Spark Context and driver and orchestrates execution of Spark Jobs. 
The Data server is the work horse - manages all in-memory data, OLTP execution engine and Spark executors. 

See the  [‘Getting Started’](../index.md) section for more details. 

From the product install directory run this script ..

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
On the `snappy-shell` prompt  …

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

Next, we recommend going through more in-depth examples in [Getting Started](../index.md). Here you will find more details on the 
concepts and experience SnappyData’s AQP, Stream analytics functionality both using SQL and Spark API.
You can also go through our very preliminary [docs](../) and provide us your comments. 

If you are interested in contributing please visit the [contributor page](contribution) for ways in which you can help. 



