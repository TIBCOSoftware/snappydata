
[![Join the chat at https://gitter.im/SnappyDataInc/snappydata](https://badges.gitter.im/SnappyDataInc/snappydata.svg)](https://gitter.im/SnappyDataInc/snappydata?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### SnappyData fuses Apache Spark with an in-memory database to deliver a data engine capable of processing streams, transactions and interactive analytics in a single cluster.

### The Challenge with Spark and remote data sources
Spark is a general purpose parallel computational engine for analytics at scale. At its core, it has a batch design center and is capable of working with disparate data sources. While, this provides rich unified access to data this can also be quite in-efficient and expensive. Analytic processing requires massive data sets to be repeatedly copied, data reformatted to suit Spark and ultimately fails to deliver the promise of interactive analytic performance, in many cases. For instance, running a aggregation on a large Cassandra table will necessitate streaming the entire table into Spark to do the aggregation, each time. Caching within Spark is immutable and would result in stale insight. 

### The SnappyData Approach
Instead, we take a very different approach. SnappyData fuses an low latency, highly available in-memory transactional database (GemFireXD) into Spark with shared memory management and optimizations. Data in the highly available in-memory store is laid out using the same columnar format as Spark(Tungsten). All query engine operators are significantly more optimized through better vectorization and code generation. The net effect is an order of magnitude performance improvement even compared to native spark caching and more than 2 orders of magnitude better Spark performance when working with external data sources. 

Essentially, we turn Spark into a in-memory operational database capable of transactions, point reads, writes, working with Streams (Spark) and running analytic SQL queries. Or, it is a in-memory scale out Hybrid Database that can execute Spark code, SQL or even Objects. 

** IF YOU ARE ALREADY USING SPARK, EXPERIENCE 10X(??) SPEED UP FOR YOUR QUERY PERFORMANCE. TRY OUT THIS [TEST](LINK TO AMAZING SPEED TEST)
// ADD FINAL 10X OR 20X SPEED UP NUMBER ONCE THE FINAL BUILD IS DONE.

##### Snappy Architecture (Resize this image)
![SnappyData Architecture](https://drive.google.com/uc?export=view&id=0B6s-Dkb7LKolaE1hS2V2SEF2NUE)

## Getting Started
You have multiple options to get going with Snappydata. Easiest option is if you are already using Spark 2.0+. You can simply get going by adding Snappydata as a package dependency. See all the options for running SnappyData [here](PROVIDE APPROPRIATE LINK)

## 5 minute Quickstart
(Again provide the same link as above)

## Documentation
The current documentation can be found [here](I am here, there, everywhere)

// SHOULD WE REMOVE THIS SECTION? ... ALREADY IN THE DOCS
## Download binary distribution
You can download the latest version of SnappyData from the [releases](https://github.com/SnappyDataInc/snappydata/releases) page. 

SnappyData has been tested on Linux and Mac OSX. If not already installed, you will need to download [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html). 

## Community Support

We monitor channels listed below for comments/questions.

[Stackoverflow](http://stackoverflow.com/questions/tagged/snappydata) ![Stackoverflow](http://i.imgur.com/LPIdp12.png)    [Slack](http://snappydata-slackin.herokuapp.com/)![Slack](http://i.imgur.com/h3sc6GM.png)        [Gitter](https://gitter.im/SnappyDataInc/snappydata) ![Gitter](http://i.imgur.com/jNAJeOn.jpg)          [Mailing List](https://groups.google.com/forum/#!forum/snappydata-user) ![Mailing List](http://i.imgur.com/YomdH4s.png)             [Reddit](https://www.reddit.com/r/snappydata) ![Reddit](http://i.imgur.com/AB3cVtj.png)          [JIRA](https://jira.snappydata.io/projects/SNAP/issues) ![JIRA](http://i.imgur.com/E92zntA.png)

## Link with SnappyData distribution
SnappyData artifacts are hosted in Maven Central. You can add a Maven dependency with the following coordinates:
```
groupId: io.snappydata
artifactId: snappydata-core_2.11
version: 0.7

groupId: io.snappydata
artifactId: snappydata-cluster_2.11
version: 0.7
```

If you are using sbt, add this line to your build.sbt for core snappy artifacts:

`libraryDependencies += "io.snappydata" % "snappydata-core_2.11" % "0.7"`

For additions related to SnappyData cluster, use:

`libraryDependencies += "io.snappydata" % "snappydata-cluster_2.11" % "0.7"`

Check out more specific SnappyData artifacts here: http://mvnrepository.com/artifact/io.snappydata

// IS THE BELOW ALSO COVERED IN THE DOCS ... IF SO, REMOVE FROM HERE. 
## Working with SnappyData Source Code
If you are interested in working with the latest code or contributing to SnappyData development, you can also check out the master branch from Git:
```
Master development branch
git clone https://github.com/SnappyDataInc/snappydata.git --recursive

###### 0.6 release branch with stability and other fixes ######
git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.6 --recursive
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

## Ad Analytics using SnappyData
Here is a stream + Transactions + Analytics use case example to illustrate the SQL as well as the Spark programming approaches in SnappyData - [Ad Analytics code example](https://github.com/SnappyDataInc/snappy-poc). Here is a [screencast](https://www.youtube.com/watch?v=bXofwFtmHjE) that showcases many useful features of SnappyData.
The example also goes through an benchmark comparing SnappyData to a Hybrid in-memory database and Cassandra. 

## Contributing to SnappyData

If you are interested in contributing, please visit the [contributor page](http://www.snappydata.io/community/contributors) for ways in which you can help.

