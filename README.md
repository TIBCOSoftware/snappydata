## Build Quickstart

As of now, only the "integrated" build seems to work. Quickstart to compile
project:

1. git clone git@github.com:SnappyData/snappy-commons.git
2. cd snappy-commons
3. git clone git@github.com:SnappyData/snappy-spark.git
4. git clone git@github.com:gemfire/gemxd-staging.git
5. git clone git@github.com:SnappyData/snappy-aqp.git
6. mv gemxd-staging snappy-store
7. git submodule init        # get the job server integrated
8. git submodule update      # checkout the correct job server code
9. ./gradlew clean assemble

## Repository layout

There were few proposals about how to manage the various repositories mentioned in [this document](https://docs.google.com/document/d/1jC8z-WPzK0B8J6p3jverumK4gcbprmFiciXYKd2JUVE/edit#). Based on few discussions, we shortlisted Proposal 4 in the document.

According to "Proposal 4" gemxd and snappy-spark repositories will be independent of any other repository. There will be a third repository that will hold the code of Snappy - snappy-commons. Snappy-Commons will have two projects:

(a) **snappy-core** - Any code that is an extension to Spark code and is not dependent on gemxd, job server etc. should go in here. For e.g. SnappyContext, cluster manager etc.

(b) **snappy-tools** - This is the code that serves as the bridge between GemXD and snappy-spark.  For e.g. query routing, job server initialization etc.

Code in snappy-tools can depend on snappy-core but it cannot happen other way round.

The snappy-spark repository has to be copied or moved inside snappy-commons for an integrated build.

(c) **snappy-spark** - This is the Spark code with Snappy modifcations.

Similarly the GemfireXD repository can be copied or moved inside snappy-commons by name *snappy-store* for an integrated build with GemFireXD. The branch of GemFireXD to use is also *snappy-store* that has been branched from rebrand_Dec13 recently for this purpose.

(d) **snappy-store** - This is the GemFireXD with Snappy additions.

(e) **snappy-aqp** - This is the Snappy Data proprietary code (AQP error estimation)

Note that git operations have still to be done separately on snappy-commons, snappy-spark and snappy-store(GemFireXD) repositories.


## Building using gradle

Gradle builds have been arranged in a way so that all of snappy projects including snappy's spark variant can be built from the top-level. In addition snappy-spark and GemFireXD (inside snappy-store) can also be built separately. If the snappy-spark directory is not present inside snappy-commons, then it will try to use locally published snappy-spark artifacts instead. Likewise if there is no snappy-store directory, then it will use the local artifacts inside local-repo in snappy-commons:
  * The full build and Intellij import has been tested with only JDK7. If you are using JDK8, then you are on your own (though it will likely work). On Ubuntu/Mint systems, best way to get Oracle JDK7 as default:

    - add webupd8 java repository: sudo add-apt-repository ppa:webupd8team/java
    - install and set jdk7 as default: sudo aptitude install oracle-java7-set-default
    - you can also install oracle-java7-unlimited-jce-policy package for enhanced JCE encryption
    - this will set java to point to JDK7 version and also set JAVA_HOME, so start a new shell for the changes to take effect; also run "source /etc/profile.d/jdk.sh" to update JAVA_HOME (or else you will need to logoff and login again for the JAVA_HOME setting to get applied)

  * Ensure that snappy-spark repository has been moved/cloned inside snappy-commons by "snappy-spark" name. Similarly move the GemFireXD (snappy-store branch) repository inside snappy-commons by "snappy-store" name. The integrated build depends on its name and presence inside else it will use the local artifacts as mentioned before. *DO NOT JUST SYMLINK THE DIRECTORIES* -- that is known to cause trouble with IDE though command-line build may go through.
  * Update both repos (snappy-commons and snappy-spark) to latest version and the GemFireXD repository in snappy-store to latest snappy-store branch. Then test the build with: ./gradlew clean && ./gradlew assemble
  * If you see an error like "Could not find hadoop-common-tests.jar", then clear maven cache artifacts: rm -rf ~/.m2/repository/org/apache/hadoop, so that gradle can download all required depedencies, then run assemble target again.
  * Run a snappy-core test application: ./gradlew :snappy-core_2.10:run -PmainClass=io.snappydata.app.SparkSQLTest
    AND/OR a GemFireXD junit test: ./gradlew :snappy-store:gemfirexd:tools:test -Dtest.single=\*\*/BugsTest


## Setting up Intellij with gradle

If the build works fine, then import into Intellij:
  * Update Intellij to the latest version, including the latest Scala plugin. Check using "Help->Check for Update". The scala plugin version in File->Settings->Plugins->Scala should be at least 1.5.4 else update the plugin from that page.
  * Double check that Scala plugin is enabled in File->Settings->Plugins, as also the Gradle plugin. Note that update in previous step could have disabled either or both, so don't assume it would be enabled.
  * Select import project, then point to the snappy-commons directory. Use external Gradle import. Add -XX:MaxPermSize=350m to VM options in global Gradle settings. Select defaults, next, next ... finish. Ignore "Gradle location is unknown warning". Ensure that a JDK7 installation has been selected.
  * Disable the "Unindexed remote maven repositories found" warning message.
  * Once import finishes, go to File->Settings->Editor->Code Style->Scala. Set the scheme as "Project". Check that the same has been set in Java's Code Style too. Then OK to close it. Next copy codeStyleSettings.xml in snappy-commons to .idea directory created by Intellij and then File->Synchronize just to be sure. Check that settings are now applied in File->Settings->Editor->Code Style->Java which should show TabSize, Indent as 2 and continuation indent as 4 (same for Scala).
  * If the Gradle tab is not visible immediately, then select it from window list popup at the left-bottom corner of IDE. If you click on that window list icon, then the tabs will appear permanently.
  * Generate avro and GemFireXD required sources by expanding :snappy-commons_2.10->Tasks->other. Right click on "generateSources" and run it. The Run item may not be available if indexing is still in progress, so wait for it to finish. The first run may take a while as it downloads jars etc. This step has to be done the first time, or if ./gradlew clean has been run, or you have made changes to javacc/avro/messages.xml source files. *IF YOU GET UNEXPECTED DATABASE NOT FOUND OR NPE ERRORS IN GemFireXD LAYER, THEN FIRST THING TO TRY IS TO RUN THE generateSources TARGET AGAIN.*
  * Increase the compiler heap sizes else build can take quite long especially with integrated GemFireXD. In File->Settings->Build, Execution, Deployment->Compiler increase "Build process heap size" to say 1536 or 2048. Similarly increase JVM maximum heap size in "Languages & Frameworks->Scala Compiler Server" to 1536 or 2048.
  * Test the full build.
  * Open Run->Edit Configurations. Expand Defaults, and select Application. Add -XX:MaxPermSize=350m in VM options. Similarly add it to VM parameters for ScalaTest and JUnit.
  * For JUnit configuration also append "/build-artifacts" to the working directory i.e. the directory should be "$MODULE_DIR$/build-artifacts". Likewise change working directory for ScalaTest to be inside build-artifacts otherwise all intermediate log and other files (especially created by GemFireXD) will pollute the source tree and may need to cleaned manually.
  * Try Run->Run... on a test like SparkSQLTest.


### Running a junit/scalatest

Running an application like SparkSQLTest should be straightforward -- just ensure that MaxPermSize has been increased as mentioned above especially for Spark/Snappy tests. For running junit/scalatest:

 * When selecting a run configuration for junit/scalatest, avoid selecting the gradle one (green round icon) otherwise that will launch an external gradle process that will start building the project all over again. Use the normal junit (red+green arrows icon) or scalatest (junit like with red overlay).
 * For JUnit tests, ensure that working directory is "$MODULE_DIR$/build-artifacts" as mentioned before. Otherwise many GemFireXD tests will fail to find the resource files required in many tests. They will also pollute the checkouts with large number of log files etc, so this will allow those to go into build-artifacts that can also be cleaned up easily.


### Manual sources and docs imports

If sources and docs were selected during initial import, then it can take a long time to get sources+docs for all dependencies. Instead one way could be to get the sources+docs for only scala-lang jars. The project setup after import already links sources and javadocs to appropriate locations in .m2 local cache, but since sources+docs were not selected during import so Maven may not have downloaded them yet. Check if you already have sources in m2 cache by opening a scala-lang class like Seq (hit Shift->Ctrl->T when using eclipse bindings and type scala.collection.Seq) and check if sources+docs are correctly shown. If not, then to easily download for selected jars do this:
  * Open the File->Project Structure->Libraries
  * Click on the '+' sign at the top to add new library, and choose Maven.
  * In the box, provide "scala-library-2.10.4" and click on the search tool.
  * Select the "org.scala-lang:scala-library:2.10.4" in the drop down. Then check "Sources", "JavaDocs" options and go ahead.
  * Do the same for others like "scala-reflect-2.10.4" and "scala-compiler-2.10.4" as required.
  * Once this is done, don't select OK on the main Project Structure box. Instead hit "Cancel" and it should be all good since we only wanted to get Maven to download the sources and docs for these jars.


## Git configuration to use keyring/keychain

Snappy is currently hosting private repositories and will continue to do
so for foreseable future. One way to avoid passing credentials everytime could
have been to upload the public SSH key and use git:// URL. However, that doesn't
work at least in Pune network due to firewall issue (and proxy server not
supporting proxying ssh). However, it is possible to configure git to enable
using gnome-keyring on Linux platforms, and KeyChain on OSX to avoid it.
(sumedh: latter not verified by me yet, so someone who uses OSX should do it)

On Linux Ubuntu/Mint:

Install gnome-keyring dev files: sudo aptitude install libgnome-keyring-dev

Build git-credential-gnome-keyring:

    cd build/git-gnome-keyring
    make

Copy to PATH (optional):

    sudo cp git-credential-gnome-keyring /usr/local/bin
    make clean

Note that if you skip this step then need to give full path in the next
step i.e. /path-to-snappy-commons/build/git-gnome-keyring/git-credential-gnome-keyring

Configure git: git config --global credential.helper gnome-keyring

Similarly on OSX locate git-credential-osxkeychain, build it if not present
(it is named "osxkeychain" instead of gnome-keyring), then set in git config.

Now your git password will be stored in keyring/keychain which is normally
unlocked automatically on login (or you will be asked to unlock on first use).

On Linux, you can install "seahorse", if not already, to see/modify all
the passwords in keyring (GUI menu "Passwords and Keys" under Preferences
or Accessories or System Tools)
