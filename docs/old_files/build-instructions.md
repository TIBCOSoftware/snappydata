## Build Quickstart

Building SnappyData requires JDK 7+ installation ([Oracle Java SE](http://www.oracle.com/technetwork/java/javase/downloads/index.html)). Quickstart to build all components of snappydata:

Latest release branch
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.9 --recursive
> cd snappydata
> ./gradlew product
```

Master
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git --recursive
> cd snappydata
> ./gradlew product
```

The product will be in _build-artifacts/scala-2.11/snappy_

If you want to build only the top-level snappydata project but pull in jars for other projects (_spark_, _store_, _spark-jobserver_):

Latest release branch
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git -b branch-0.9
> cd snappydata
> ./gradlew product
```

Master
```sh
> git clone https://github.com/SnappyDataInc/snappydata.git
> cd snappydata
> ./gradlew product
```


## Repository layout

- **core** - Extensions to Apache Spark that should not be dependent on SnappyData Spark additions, job server etc. It is also the bridge between _spark_ and _store_ (GemFireXD). For example: SnappyContext, row and column store, streaming additions etc.

- **cluster** - Provides the SnappyData implementation of cluster manager embedding GemFireXD, query routing, job server initialization etc.

  This component depends on _core_ and _store_. Code in _cluster_ depends on _core_ but not the other way round.

- **spark** - _Apache Spark_ code with SnappyData enhancements.

- **store** - Fork of gemfirexd-oss with SnappyData additions on the snappy/master branch.

- **spark-jobserver** - Fork of _spark-jobserver_ project with some additions to integrate with SnappyData.

  The _spark_, _store_ and _spark-jobserver_ directories are required to be clones of the respective SnappyData repositories, and are integrated in the top-level snappydata project as git submodules. When working with submodules, updating the repositories follows the normal [git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules). One can add some aliases in gitconfig to aid pull/push like:

```
[alias]
  spull = !git pull && git submodule sync --recursive && git submodule update --init --recursive
  spush = push --recurse-submodules=on-demand
```

The above aliases can serve as useful shortcuts to pull and push all projects from top-level _snappydata_ repository.


## Building

Gradle is the build tool used for all the SnappyData projects. Changes to _Apache Spark_ and _spark-jobserver_ forks include addition of gradle build scripts to allow building them independently as well as a subproject of snappydata. The only requirement for the build is a JDK 7+ installation. Currently most of the testing has been with JDK 7. The gradlew wrapper script will download all the other build dependencies as required.

If a user does not want to deal with submodules and only work on snappydata project, then can clone only the snappydata repository (without the --recursive option) and the build will pull those SnappyData project jar dependencies from maven central.

If working on all the separate projects integrated inside the top-level snappydata clone, the gradle build will recognize the same and build those projects too and include the same in the top-level product distribution jar. The _spark_ and _store_ submodules can also be built and published independently.

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

The default build directory is _build-artifacts/scala-2.11_ for projects. Exception is _store_ project, where the default build directory is _build-artifacts/;os;_ where _;os;_ is _linux_ on Linux systems, _osx_ on Mac, _windows_ on Windows.

The usual gradle test run targets (_test_, _check_) work as expected for junit tests. Separate targets have been provided for running scala tests (_scalaTest_) while the _check_ target will run both the junit and scalatests. One can run a single scala test suite class with _singleSuite_ option while running a single test within some suite works with the _--tests_ option:

```sh
> ./gradlew core:scalaTest -PsingleSuite=**.ColumnTableTest  # run all tests in the class
> ./gradlew core:scalaTest \
>    --tests "Test the creation/dropping of table using SQL"  # run a single test (use full name)
```
Running individual tests within some suite works using the _--tests_ argument.


## Setting up Intellij with gradle

Intellij is the IDE commonly used by the snappydata developers. Those who really prefer Eclipse can try the scala-IDE and gradle support, but has been seen to not work as well (e.g. gradle support is not integrated with scala plugin etc).  To import into Intellij:

- Update Intellij to the latest version, including the latest Scala plugin. Older versions have trouble dealing with scala code particularly some of the code in _spark_.
- Increase the Xmx to 2g or more (4g if possible) in the IDEA global vmoptions (in product bin directory, files named idea64.vmoptions for 64-bit and idea.vmoptions for 32-bit).
- If using Java 8 release 144 or later, also add -Djdk.util.zip.ensureTrailingSlash=false to the global vmoptions file to fix an IDEA issue (https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000754864--SOLVED-No-default-file-and-code-templates). Not required for the latest IDEA releases.
- Select import project, then point to the snappydata directory. Use external Gradle import. Unselect "Create separate module per source set" option.
- When using JDK 7, add _-XX:MaxPermSize=350m_ to VM options in global Gradle settings. Select defaults, next, next ... finish. Ignore _"Gradle location is unknown warning"_. Ensure that a JDK 7/8 installation has been selected. Ignore and dismiss the _"Unindexed remote maven repositories found"_ warning message, if seen.
- Once import finishes, go to _File->Settings->Editor->Code Style->Scala_. Set the scheme as _Project_. Check that the same has been set in Java Code Style too. Then OK to close it. Next copy _codeStyleSettings.xml_ in snappydata top-level directory to .idea directory created by Intellij. Check that settings are now applied in _File->Settings->Editor->Code Style->Java_ which should show Indent as 2 and continuation indent as 4 (same for Scala).
- If the Gradle tab is not visible immediately, then select it from window list popup at the left-bottom corner of IDE. If you click on that window list icon, then the tabs will appear permanently.
- Generate avro and GemFireXD required sources by expanding: _snappydata_2.11->Tasks->other_. Right click on _generateSources_ and run it. The Run item may not be available if indexing is still in progress, so wait for it to finish. The first run may take a while as it downloads jars etc. This step has to be done the first time, or if _./gradlew clean_ has been run, or you have made changes to _javacc/avro/messages.xml_ source files. *If you get unexpected _"Database not found"_ or _NullPointerException_ errors in GemFireXD layer, then first thing to try is to run the _generateSources_ target again.*
- Increase the compiler heap sizes or else the build can take quite long especially with integrated _spark_ and _store_. In _File->Settings->Build, Execution, Deployment->Compiler increase _, _Build process heap size_ to say 1536 or 2048. Similarly increase JVM maximum heap size in _Languages & Frameworks->Scala Compiler Server_ to 1536 or 2048.
- Test the full build.
- For JDK 7: _Open Run->Edit Configurations_. Expand Defaults, and select Application. Add _-XX:MaxPermSize=350m_ in VM options. Similarly add it to VM parameters for ScalaTest and JUnit. Most of unit tests will have trouble without this option.
- For JUnit configuration also append _/build-artifacts_ to the working directory i.e. the directory should be _\$MODULE_DIR\$/build-artifacts_. Likewise change working directory for ScalaTest to be inside _build-artifacts_ otherwise all intermediate log and other files (especially created by GemFireXD) will pollute the source tree and may need to cleaned manually.
- If you see below error during building the project, open module settings, select the module snappy-cluster_2.11, go to its Dependencies tab and ensure that snappy-spark-unsafe_2.11 comes before spark-unsafe (or just find snappy-spark-unsafe_2.11 and move it to top).

```
Error:(236, 18) value getByte is not a member of org.apache.spark.unsafe.types.UTF8String
      if (source.getByte(i) == first && matchAt(source, target, i)) return true
Error:(233, 24) value getByte is not a member of org.apache.spark.unsafe.types.UTF8String
    val first = target.getByte(0)
```


### Running a scalatest/junit

Running scala/junit tests from Intellij should be straightforward -- just ensure that MaxPermSize has been increased when using JDK 7 as mentioned above especially for Spark/Snappy tests.
- When selecting a run configuration for junit/scalatest, avoid selecting the gradle one (green round icon) otherwise that will launch an external gradle process that can start building the project again and won't be cleanly integrated with Intellij. Use the normal junit (red+green arrows icon) or scalatest (junit like with red overlay).
- For JUnit tests, ensure that working directory is _\$MODULE_DIR\$/build-artifacts_ as mentioned before. Otherwise many GemFireXD tests will fail to find the resource files required in tests. They will also pollute the checkouts with log files etc, so this will allow those to go into build-artifacts that is easier to clean. For that reason is may be preferable to do the same for scalatests.
- Some of the tests use data files from the _tests-common_ directory. For such tests, run the gradle task _snappydata_2.11->Tasks->other->copyResourcesAll_ to copy the resources in build area where Intellij runs can find it.

