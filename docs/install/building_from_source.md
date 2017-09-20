<a id="building-from-source"></a>
# Building from Source
Building SnappyData requires JDK 8 installation ([Oracle Java SE](http://www.oracle.com/technetwork/java/javase/downloads/index.html)).

## Build all components of SnappyData
 
**Latest release branch**
```bash
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-<release-version> --recursive
> cd snappydata
> ./gradlew product
```

**Master**
```bash
> git clone https://github.com/SnappyDataInc/snappydata.git --recursive
> cd snappydata
> ./gradlew product
```

The product is in **build-artifacts/scala-2.11/snappy**

## Build only the top-level components

Use this option if you want to build only the top-level SnappyData project and pull in jars for other projects (spark, store, spark-jobserver):

**Latest release branch**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-<release-version>
> cd snappydata
> ./gradlew product
```

**Master**
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git
> cd snappydata
> ./gradlew product
```


## Repository layout

- **core** - Extensions to Apache Spark that should not be dependent on SnappyData Spark additions, job server etc. It is also the bridge between _spark_ and _store_ (GemFireXD). For example, SnappyContext, row and column store, streaming additions etc.

- **cluster** - Provides the SnappyData implementation of cluster manager embedding GemFireXD, query routing, job server initialization etc.

  This component depends on _core_ and _store_.  The code in the _cluster_ depends on the _core_ but not the other way round.

- **spark** - _Apache Spark_ code with SnappyData enhancements.

- **store** - Fork of gemfirexd-oss with SnappyData additions on the snappy/master branch.

- **spark-jobserver** - Fork of _spark-jobserver_ project with some additions to integrate with SnappyData.

  The _spark_, _store_, and _spark-jobserver_ directories are required to be clones of the respective SnappyData repositories and are integrated into the top-level SnappyData project as git submodules. When working with submodules, updating the repositories follows the normal [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules). One can add some aliases in gitconfig to aid pull/push like:

```
[alias]
  spull = !git pull && git submodule sync --recursive && git submodule update --init --recursive
  spush = push --recurse-submodules=on-demand
```

The above aliases can serve as useful shortcuts to pull and push all projects from top-level _snappydata_ repository.


## Building

Gradle is the build tool used for all the SnappyData projects. Changes to _Apache Spark_ and _spark-jobserver_ forks include the addition of Gradle build scripts to allow building them independently as well as a sub-project of SnappyData. The only requirement for the build is a JDK 8 installation. The Gradle wrapper script downloads all the other build dependencies as required.

If you do not want to deal with sub-modules and only work on a SnappyData project, you can clone only the SnappyData repository (without the `--recursive` option) and the build pulls those SnappyData project jar dependencies from Maven central.

If working on all the separate projects integrated inside the top-level SnappyData clone, the Gradle build recognizes the same and build those projects too and include the same in the top-level product distribution jar. The *spark* and *store* submodules can also be built and published independently.

Useful build and test targets:

```
./gradlew assemble      -  build all the sources
./gradlew testClasses   -  build all the tests
./gradlew product       -  build and place the product distribution
                           (in build-artifacts/scala_2.11/snappy)
./gradlew distTar       -  create a tar.gz archive of product distribution
                           (in build-artifacts/scala_2.11/distributions)
./gradlew distZip       -  create a zip archive of product distribution
                           (in build-artifacts/scala_2.11/distributions)
./gradlew buildAll      -  build all sources, tests, product, packages (all targets above)
./gradlew checkAll      -  run testsuites of snappydata components
./gradlew cleanAll      -  clean all build and test output
./gradlew runQuickstart -  run the quickstart suite (the "Getting Started" section of docs)
./gradlew precheckin    -  cleanAll, buildAll, scalaStyle, build docs,
                           and run full snappydata testsuite including quickstart
./gradlew precheckin -Pstore  -  cleanAll, buildAll, scalaStyle, build docs,
                           run full snappydata testsuite including quickstart
                           and also full SnappyData store testsuite
```

The default build directory is _build-artifacts/scala-2.11_ for projects. An exception is *store* project, where the default build directory is *build-artifacts/;os*; where; *;os;* is *linux* on Linux systems, *osx* on Mac, *windows* on Windows.

The usual Gradle test run targets (_test_, _check_) work as expected for JUnit tests. Separate targets have been provided for running Scala tests (_scalaTest_) while the _check_ target runs both the JUnit and ScalaTests. One can run a single Scala test suite class with _singleSuite_ option while running a single test within some suite works with the _--tests_ option:

```sh
> ./gradlew core:scalaTest -PsingleSuite=**.ColumnTableTest  # run all tests in the class
> ./gradlew core:scalaTest \
>    --tests "Test the creation/dropping of table using SQL"  # run a single test (use full name)
```
Running individual tests within some suite works using the _--tests_ argument.


## Setting up IntelliJ IDEA with Gradle

IntelliJ IDEA is the IDE commonly used by developers at SnappyData. Users who prefer to use Eclipse can try the Scala-IDE and Gradle support, however, it is recommended to use IntelliJ IDEA. </br>
Steps required for setting up SnappyData with all its components in IDEA are listed below.

To import into IntelliJ IDEA:

* Upgrade IntelliJ IDEA to version 2016.x, including the latest Scala plug-in. Older versions (pre 14.x) have trouble dealing with Scala code, particularly some of the code in Spark. Ensure JDK 8 is installed and IDEA can find it (either in PATH or via *JAVA_HOME*).

* Increase the available JVM heap size for IDEA. Open bin/idea64.vmoptions (assuming 64-bit JVM) and increase -Xmx option to be something like *-Xmx=2g* for comfortable use.

* Select **Import Project**, and then select the SnappyData directory. Use external Gradle import. Clear the **Create separate module per source set** option, while other options can continue with the default . Click **Next** in the following screens.<br/>
    
	!!! Note:
		
        * Ignore the **"Gradle location is unknown warning"**.

        * Ensure that the JDK 8 installation has been selected.

        * Ignore and dismiss the **"Unindexed remote Maven repositories found"** warning message if seen.

* When import is completed, 
	1. Go to **File> Settings> Editor> Code Style> Scala**. Set the scheme as **Project**. 
	2. In the same window, select **Java** code style and set the scheme as **Project**. 
	3. Click **OK** to apply and close the window. 
	4. Copy **codeStyleSettings.xml** located in the SnappyData top-level directory, to the **.idea** directory created by IDEA. 
	5. Verify that the settings are now applied in **File> Settings> Editor> Code Style> Java** which should display indent as 2 and continuation indent as 4 (same as Scala).

* If the Gradle tab is not visible immediately, then select it from option avaiable at the bottom-left of IDE. If you click on that window list icon, then the tabs are displayed permanently.

* Generate Apache Avro and SnappyData required sources by expanding: **snappydata_2.11> Tasks> other**. Right-click on **generateSources** and run it. The **Run** option may not be available if indexing is still in progress, wait for indexing to complete, and then try again. <br> The first run may take some time to complete, as it downloads the jar files and other required files. This step has to be done the first time, or if **./gradlew clean** has been run, or if you have made changes to **javacc/avro/messages.xml** source files.

* If you get unexpected **"Database not found"** or **NullPointerException** errors in SnappyData-store/GemFireXD layer, run the **generateSources** target (Gradle tab) again.

* If you get **NullPointerException** error when reading the spark-version-info.properties file, right-click and run the **copyResourcesAll** target from **snappydata_2.11> Tasks> other** (Gradle tab) to copy the required resources.

* Increase the compiler heap sizes or else the build can take a long to completely, especially with integrated *spark* and *store*. In **File> Settings> Build, Execution, Deployment> Compiler** option increase the **Build process heap size** to 1536 or 2048. Similarly, in **Languages & Frameworks> Scala Compiler Server** option, increase the JVM maximum heap size to 1536 or 2048.

* Test the full build.

* For JUnit tests configuration also append **/build-artifacts** to the working directory. That is, open **Run> Edit Configurations**, expand **Defaults** and select **JUnit**, the working directory should be **\$MODULE_DIR\$/build-artifacts**. Likewise, append **build-artifacts** to working directory for ScalaTest. Without this, all intermediate log and other files pollute the source tree and will have to be cleaned manually.

## Running a ScalaTest/JUnit

Running Scala/JUnit tests from IntelliJ IDEA is straightforward.

* When selecting a run configuration for JUnit/ScalaTest, avoid selecting the Gradle one (green round icon) otherwise, an external Gradle process is launched that can start building the project again and will not be cleanly integrated with IDEA. Use the normal JUnit (red+green arrows icon) or ScalaTest (JUnit like with red overlay).

* For JUnit tests, ensure that the working directory is the top-level **\$MODULE_DIR\$/build-artifacts** as mentioned earlier. Otherwise, many SnappyData-store tests fail to find the resource files required in tests. They also pollute the files, so when launched this allows those to go into **build-artifacts** that are easier to clean. For that reason, it is preferable to do the same for ScalaTests.

* Some of the tests use data files from the **tests-common** directory. For such tests, run the Gradle task **snappydata_2.11> Tasks> other> copyResourcesAll** to copy the resources in build area where IDEA runs can find it.
