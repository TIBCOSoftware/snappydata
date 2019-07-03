# SnapyData Examples

This examples directory, a sub-project in SnappyData repository, has sample application programs which can be run as jobs on a SnappyData cluster or also as Spark jobs.
There are variety of examples for different scenarios like batch and streaming processing, embedded-mode, smartconnector-mode, joins involving different types of tables.
It also has a test suite which validates these jobs.


## Set up the project in IDE

Users can import the examples directory as an independent gradle project in an IDE for you to easily modify and run the examples.
You may follow below steps to setup a project in Intellij IDEA.
These have been tried on Intellij IDEA 2018.2.7, with java version 1.8.0_201, Scala plugin 2018.2.11 (Scala version 2.11.11) and Gradle plugin.

1. Copy examples directory to your local filesystem where you want to create the new project.
2. Open IDEA Intellij. Do `File ->  New -> Project from Existing Sources`. Select examples/ as the base project directory.

## Building the project

Execute gradle task `build` from Gradle Tool Window of your IDE to verify the setup.
Alternatively, you can also run `gradle build` in your terminal from the base project directory if you have the Gradle installed.

It compiles all the example source code and packages the classes into a jar named snappydata-examples_2.11-<version>.jar in `build/libs/` directory.

## Running the examples

You can run most of the examples directly from the IDE itself. For others, you can run them as jobs in SnappyData cluster.

### As standalone programs (from IDE)

The example programs which have the `main(...)` method defined can be run directly from the IDE.

To do that, simply open the relevant scala file in your IDE and then right-click -> click on 'Run ...'

e.g.
Open CreateReplicatedRowTable.scala, right-click and click on `Run 'CreateReplicatedRowTable'`.

Similarly, you can also run the test suite (ExampleTestSuite.scala) from IDE, provided you specify path of your SnappyData installation directory as value for the environment variable `SNAPPY_HOME` in its Run Configuration.

**NOTE:** The examples in StructuredStreamingCDCExample.scala and SynopsisDataExample.scala showcase enterprise features which are not available in Community Edition of SnappyData.

### As jobs (from console).

You can run the example programs as jobs in the SnappyData cluster using either snappy-job.sh script ([Embedded mode](https://snappydatainc.github.io/snappydata/affinity_modes/embedded_mode/)) or spark-submit script ([Smart Connector mode](https://snappydatainc.github.io/snappydata/affinity_modes/connector_mode/)).

- The examples which can be run as a job in Embedded mode do extend one of `SnappySQLJob`, `SnappyStreamingJob` and `JavaSnappySQLJob`.
  Below command runs `CreateAndLoadAirlineDataJob` as a SnappyData job in Embedded mode.
  Note that the jar `snappydata-examples_2.11-1.1.0-HF-1.jar` specified via `--app-jar` below is created by the gradle task `build` stated earlier.

  This assumes that SnappyData cluster is running on localhost with default settings and `SNAPPY_HOME` points to your SnappyData installation directory.

  ```
  $SNAPPY_HOME/bin/snappy-job.sh submit --lead localhost:8090 --app-name CreateAndLoadAirlineDataJob --class io.snappydata.examples.CreateAndLoadAirlineDataJob --app-jar build/libs/snappydata-examples_2.11-1.1.0-HF-1.jar
  ```

- The example programs with `main(...)` method defined can be run via `bin/spark-submit`, which runs it in Smart Connector mode.
  Below command runs `AirlineDataSparkApp` as a job in Smart Connector mode. You need to specify Spark master url as value for `--master` below.

  ```
  $SNAPPY_HOME/bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp \
  --master spark://<hostname>:7077 --conf snappydata.connection=localhost:1527 \
  build/libs/snappydata-examples_2.11-1.1.0-HF-1.jar`
  ```

  **NOTE:** The Spark cluster must have essential SnappyData classes in its classpath so that it can talk to SnappyData cluster in Smart Connector mode.
  If you use SnappyData Spark distribution to launch the Spark cluster (`$SNAPPY_HOME/sbin/start-all.sh`), these are already present via its `jars/` directory.

  If you use Apache Spark 2.1.1 distribution, you can add the required SnappyData classes into its classpath via `--packages` option as shown below.

  ```
  $SNAPPY_HOME/bin/spark-submit --class io.snappydata.examples.AirlineDataSparkApp \
  --master spark://<hostname>:7077 --conf snappydata.connection=localhost:1527 \
  --packages SnappyDataInc:snappydata:1.1.0-HF-1-s_2.11 \
  build/libs/snappydata-examples_2.11-1.1.0-HF-1.jar`
  ```
