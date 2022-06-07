<a id="building-from-source"></a>
# Building from Source

!!! Note
	Building SnappyData requires JDK 8 installation (like [Oracle Java SE](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or OpenJDK 8).

## Build all Components of SnappyData

**Master**

```sh
> git clone https://github.com/TIBCOSoftware/snappydata.git --recursive
> cd snappydata
> ./gradlew product
```

The product is in **build-artifacts/scala-2.11/snappy**

To build product artifacts in all supported formats (tarball, zip, rpm, deb):

```sh
> git clone https://github.com/TIBCOSoftware/snappydata.git --recursive
> cd snappydata
> ./gradlew cleanAll distProduct
```

The artifacts are in **build-artifacts/scala-2.11/distributions**

You can also add the flags `-PenablePublish -PR.enable` to get them in the form as in an official
SnappyData distributions but that also requires an installation of R as noted below.

To build all product artifacts that are in the official SnappyData distributions:

```sh
> git clone https://github.com/TIBCOSoftware/snappydata.git --recursive
> cd snappydata
> ./gradlew cleanAll product copyShadowJars distTar -PenablePublish -PR.enable
```

The artifacts are in **build-artifacts/scala-2.11/distributions**

Building SparkR with the `-PR.enable` flag requires R 3.x or 4.x to be installed locally.
At least the following R packages along with their dependencies also need to be installed:
`knitr`, `markdown`, `rmarkdown`, `e1071`, `testthat`

The R build also needs a base installation of `texlive` or an equivalent `TeX` distribution for `pdflatex` and related tools to build the documentation.

On distributions like Ubuntu/Debian/Arch you can install them from repositories (for Arch some packages need AUR). For example on recent Ubuntu/Debian:

```sh
sudo apt install texlive r-cran-knitr r-cran-markdown r-cran-rmarkdown r-cran-e1071 r-cran-testthat
```

Official builds are published to maven using the `publishMaven` task.

### Docs

Documentation is created using the `publishDocs` target. You need an installation of `mkdocs`
with its `material`, `minify` and 'mike' plugins to build and publish the docs. The standard way
to install these is using python package installer `pip`. This is present in the repositories
of all distributions e.g. `sudo apt install python3-pip` on Ubuntu/Debian based distributions
and `sudo yum install python3-pip` on CentOS/RHEL/Fedora based ones. Other distributions will
have a similar way to install the package (e.g. `sudo pacman -S python-pip` on Arch-like
    distributions).

Once you have `pip` installed, then you can install the required packages using:

```sh
pip3 install mkdocs mkdocs-material mkdocs-minify-plugin mike
```

!!!Note
    On some newer distributions, the executable is `pip` instead of `pip3` though latter is usually still available as a symlink.

Then you can run the target that will build the documentation as below:

```sh
./gradlew publishDocs -PenablePublish
```

This will publish the docs for the current release version to the public [site](https://tibcosoftware.github.io/snappydata) assuming you have the requisite write permissions to the repository. If there are docs for multiple releases, then they will appear in the drop-down at the top.

If you want to see the docs locally, then uncomment the last `mike serve` line in `publish-site.sh` and the site will be hosted locally in [localhost:8000](http://localhost:8000). Note that locally hosted site may have some subtle differences from the one published on the site, so its best to check the final published output on the site.

## Repository Layout

- **core** - Extensions to Apache Spark that should not be dependent on SnappyData Spark additions, job server etc. It is also the bridge between _spark_ and _store_ (GemFireXD). For example, SnappyContext, row and column store, streaming additions etc.

- **cluster** - Provides the SnappyData implementation of cluster manager embedding GemFireXD, query routing, job server initialization etc.

This component depends on _core_ and _store_. The code in the _cluster_ depends on the _core_ but not the other way round.

- **spark** - _Apache Spark_ code with SnappyData enhancements.

- **store** - Fork of gemfirexd-oss with SnappyData additions on the snappy/master branch.

- **spark-jobserver** - Fork of _spark-jobserver_ project with some additions to integrate with SnappyData.

- **aqp** - Approximate Query Processing (AQP) module of SnappyData.

- **snappy-connectors** - Connector for Apache Geode and a Change-Data-Capture (CDC) connector.

  The _spark_, _store_, _spark-jobserver_, _aqp_, and _snappy-connectors_ directories are required to be clones of the respective SnappyData repositories and are integrated into the top-level SnappyData project as git submodules. When working with submodules, updating the repositories follows the normal [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules). One can add some aliases in gitconfig to aid pull/push as follows:

```ini
[alias]
  spull = !git pull && git submodule sync --recursive && git submodule update --init --recursive
  spush = push --recurse-submodules=on-demand
```

The above aliases can serve as useful shortcuts to pull and push all projects from top-level _snappydata_ repository.


## Building

Gradle is the build tool used for all the SnappyData projects. Changes to _Apache Spark_ and _spark-jobserver_ forks include the addition of Gradle build scripts to allow building them independently as well as a sub-project of SnappyData. The only requirement for the build is a JDK 8 installation. The Gradle wrapper script downloads all the other build dependencies as required.

If you do not want to deal with sub-modules and only work on a SnappyData project, you can clone only the SnappyData repository (without the `--recursive` option) and the build pulls those SnappyData project jar dependencies from Maven central.

If working on all the separate projects integrated inside the top-level SnappyData clone, the Gradle build recognizes the same and build those projects too and includes the same in the top-level product distribution jar. The *spark* and *store* submodules can also be built and published independently.

Useful build and test targets:

```sh
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
./gradlew buildDtests   -  To build the Distributed tests
```

The default build directory is _build-artifacts/scala-2.11_ for projects. An exception is *store* project, where the default build directory is *_build-artifacts/<os>_*; where; *os* is *linux* on Linux systems, *osx* on Mac, *windows* on Windows.

The usual Gradle test run targets (_test_, _check_) work as expected for JUnit tests. Separate targets have been provided for running Scala tests (_scalaTest_) while the _check_ target runs both the JUnit and ScalaTests. One can run a single Scala test suite class with _singleSuite_ option while running a single test within some suite works with the `--tests` option:

```sh
> ./gradlew snappy-core:scalaTest -PsingleSuite=**.ColumnTableTest  # run all tests in the class
> ./gradlew snappy-core:scalaTest \
>    --tests "Test the creation/dropping of table using SQL"  # run a single test (use full name)
```
Running individual tests within some suite works using the `--tests` argument.

All ScalaTest build targets can be found by running the following command (case sensitive):
`./gradlew tasks --all | grep scalaTest`

## Setting up IntelliJ IDEA with Gradle

IntelliJ IDEA is the IDE commonly used by developers at SnappyData. Users who prefer to use Eclipse can try the Scala-IDE and Gradle support, however, it is recommended to use IntelliJ IDEA. </br>
Steps required for setting up SnappyData with all its components in IDEA are listed below.

To import into IntelliJ IDEA:

- Upgrade IntelliJ IDEA to at least version 2016.x, preferably 2018.x or more, including the latest Scala plug-in. Older versions have trouble dealing with scala code particularly some of the code in Spark. Newer versions have trouble running tests with gradle import, since they do not honor the build output directory as set in gradle. Ensure JDK 8 is installed and IDEA can find it (either in PATH or via JAVA_HOME).

- Increase the `Xmx` to 2g or more (4g, if possible) in the **IDEA global vmoptions** (in product bin directory, files named **idea64.vmoptions** for 64-bit and **idea.vmoptions** for 32-bit).

- If using Java 8 release 144 or later, also add **-Djdk.util.zip.ensureTrailingSlash=false** to the **global vmoptions** file to fix an [IDEA issue](https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000754864--SOLVED-No-default-file-and-code-templates).

* Increase the available JVM heap size for IDEA. Open **bin/idea64.vmoptions** (assuming 64-bit JVM) and increase `-Xmx` option to be something like **-Xmx2g** for comfortable use.

* Select **Import Project**, and then select the SnappyData directory. Use external Gradle import. Click **Next** in the following screen. Clear the **Create separate module per source set** option, while other options can continue with the default. Click **Next** in the following screens.<br/>

    !!! Note
		
        * Ignore the **"Gradle location is unknown warning"**.

        * Ensure that the JDK 8 installation has been selected.

        * Ignore and dismiss the **"Unindexed remote Maven repositories found"** warning message if seen.

* When import is completed,
    1. Go to **File> Settings> Editor> Code Style> Scala**. Set the scheme as **Project**.

    2. In the same window, select **Java** code style and set the scheme as **Project**.

    3. Click **OK** to apply and close the window.

    4. Copy **codeStyleSettings.xml** located in the SnappyData top-level directory, to the **.idea** directory created by IDEA.

    5. Verify that the settings are now applied in **File> Settings> Editor> Code Style> Java** which should display indent as 2 and continuation indent as 4 (same as Scala).

* If the Gradle tab is not visible immediately, then select it from option available at the bottom-left of IDE. Click on that window list icon for the tabs to be displayed permanently.

* Generate Apache Avro and SnappyData required sources by expanding: **snappydata_2.11> Tasks> other**. Right-click on **generateSources** and run it. The **Run** option may not be available if indexing is still in progress, wait for indexing to complete, and then try again. <br> The first run may take some time to complete, as it downloads the jar files and other required files. This step has to be done the first time, or if **./gradlew clean** has been run, or if you have made changes to **javacc/avro/messages.xml** source files.

* Go to **File> Settings> Build, Execution, Deployment> Build tools> Gradle**. Enter **-DideaBuild** in the **Gradle VM Options** textbox.

* If you get unexpected **Database not found** or **NullPointerException** errors in SnappyData-store/GemFireXD layer, run the **generateSources** target (Gradle tab) again.

* If you get **NullPointerException** error when reading the **spark-version-info.properties** file, right-click and run the **copyResourcesAll** target from **snappydata_2.11> Tasks> other** (Gradle tab) to copy the required resources.

* Increase the compiler heap sizes or else the build can take a long time to complete, especially with integrated *spark* and *store*. In **File> Settings> Build, Execution, Deployment> Compiler** option increase the **Build process heap size** to 1536 or 2048. Similarly, in **Languages & Frameworks> Scala Compiler Server** option, increase the JVM maximum heap size to 1536 or 2048.

* Test the full build.

* For JUnit tests configuration also append **/build-artifacts** to the working directory. That is, open **Run> Edit Configurations**, expand **Defaults** and select **JUnit**, the working directory should be **\$MODULE_DIR\$/build-artifacts**. Likewise, append **build-artifacts** to the working directory for ScalaTest. Without this, all intermediate log and other files pollute the source tree and will have to be cleaned manually.

* If you see the following error while building the project, open module settings, select the module **snappy-cluster_2.11**, go to its **Dependencies** tab and ensure that **snappy-spark-unsafe_2.11** comes before **spark-unsafe** or just find **snappy-spark-unsafe_2.11** and move it to the top.


```pre
Error:(236, 18) value getByte is not a member of org.apache.spark.unsafe.types.UTF8String
    if (source.getByte(i) == first && matchAt(source, target, i)) return true
    Error:(233, 24) value getByte is not a member of org.apache.spark.unsafe.types.UTF8String
    val first = target.getByte(0)
```

Even with the above, running unit tests in IDEA may result in more runtime errors due to unexpected **slf4j** versions. A more comprehensive way to correct, both the compilation and unit test problems in IDEA, is to update the snappy-cluster or for whichever module unit tests are to be run and have the **TEST** imports at the end.

The easiest way to do that is to close IDEA, open the module IML file (**.idea/modules/cluster/snappy-cluster_2.11.iml** in this case) in an editor. Search for **scope="TEST"** and move all those lines to the bottom just before `</component>` close tag.
The ordering of imports is no longer a problem in latest IDEA 2020.x and newer.

## Running a ScalaTest/JUnit

Running Scala/JUnit tests from IntelliJ IDEA is straightforward.

* In newer IDEA releases, ensure that Intellij IDEA is used to run the tests instead of gradle, while gradle
  is used for the build. To do this, go to File->Settings->Build, Execution, Deployment->Build Tools->Gradle,
  select "Run tests using" to be "Intellij IDEA" rather than with gradle for the "snappydata" project.
  Also ensure that "Build and run using" is gradle rather than "Intellij IDEA".

* When selecting a run configuration for JUnit/ScalaTest, avoid selecting the Gradle one (green round icon) otherwise, an external Gradle process is launched that can start building the project again is not cleanly integrated with IDEA. Use the normal JUnit (red+green arrows icon) or ScalaTest (JUnit like with red overlay).

* For JUnit tests, ensure that the working directory is the top-level **\$MODULE_DIR\$/build-artifacts** as mentioned earlier. Otherwise, many SnappyData-store tests fail to find the resource files required in tests. They also pollute the files, so when launched, this allows those to go into **build-artifacts** that are easier to clean. For that reason, it is preferable to do the same for ScalaTests.

* Some of the tests use data files from the **tests-common** directory. For such tests, run the Gradle task **snappydata_2.11> Tasks> other> copyResourcesAll** to copy the resources in build area where IDEA runs can find it.
