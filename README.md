## Repository layout

There were few proposals about how to manage the various repositories mentioned in [this document](https://docs.google.com/document/d/1jC8z-WPzK0B8J6p3jverumK4gcbprmFiciXYKd2JUVE/edit#). Based on few discussions, we shortlisted Proposal 4 in the document. 

According to "Proposal 4" gemxd and snappy-spark repositories will be independent of any other repository. There will be a third repository that will hold the code of Snappy - snappy-commons. Snappy-Commons will have two projects: 
 
(a) **snappy-core** - Any code that is an extension to Spark code and is not dependent on gemxd, job server etc. should go in here. For e.g. SnappyContext, cluster manager etc. 

(b) **snappy-tools** - This is the code that serves as the bridge between GemXD and snappy-spark.  For e.g. query routing, job server initialization etc. 

Code in snappy-tools can depend on snappy-core but it cannot happen other way round. 

Lastly the snappy-spark repository has to be copied or moved inside snappy-commons for an integrated build.

(c) **snappy-spark** - This is the Spark code with Snappy modifcations. 

Note that git operations have still to be done separately on snappy-commons and snappy-spark repositories.


## Building using gradle

Gradle builds have been arranged in a way so that all of snappy projects including snappy's spark variant can be built from the top-level. In addition snappy-spark can also be built separately:
  * The full build and Intellij import has been tested with only JDK7. If you are using JDK8, then you are on your own. On Ubuntu/Mint systems, best way to get Oracle JDK7 as default:

    - add webupd8 java repository: sudo add-apt-repository ppa:webupd8team/java
    - install and set jdk7 as default: sudo aptitude install oracle-java7-set-default
    - you can also install oracle-java7-unlimited-jce-policy package for enhanced JCE encryption
    - this will set java to point to JDK7 version and also set JAVA_HOME, so start a new shell for the changes to take effect; also run "source /etc/profile.d/jdk.sh" to update JAVA_HOME (or else you will need to logoff and login again for the JAVA_HOME setting to get applied)

  * Ensure that snappy-spark repository has been moved/cloned inside snappy-commons by "snappy-spark" name. The integrated build depends on its name and presence inside. DO NOT JUST SYMLINK THE DIRECTORY -- that is known to cause trouble with IDE though command-line build may go through.
  * Update both repos (snappy-commons and snappy-spark) to latest version. Then test the build with: ./gradlew clean && ./gradlew assemble
  * Run a snappy-core test application: ./gradlew :snappy-core_2.10:run -PmainClass=io.snappydata.app.SparkSQLTest (TODO: this still fails due to some runtime dependencies?)


## Setting up Intellij with gradle

If the build works fine, then import into Intellij:
  * Intellij somehow fails with scala plugin in gradle import even with very simple projects but works in later refresh fine. So first apply "patch -p0 < build/gradle-idea-hack.diff" that disables scala plugin temporarily in gradle build files.
  * First ensure that gradle plugin is enabled in Preferences->Plugins.
  * Select import project, then point to the snappy-commons directory.
  * Use external Gradle import. You could add -XX:MaxPermSize=350m to VM options in global Gradle settings. Select defaults, next, next ... finish. Ignore "Gradle location is unknown warning". Its not recommended to use auto-import since we may need to live with few manual tweaks for now (see gen-java point below).
  * Once import finishes, copy codeStyleSettings.xml in snappy-commons to .idea directory created by Intellij
  * Once initial import is done (indexing may still be on but that doesn't matter), reverse the above patch "patch -p0 -R < build/gradle-idea-hack.diff"
  * Then open the Gradle tab on the right and hit the first refresh icon. If the Gradle tab is not visible immediately, then select it from window list popup at the left-bottom corner of IDE. If you click on that window list icon, then the tabs will appear permanently.
  * Generate avro source by expanding :snappy-spark:snappy-spark-streaming-flume-sink_2.10->Tasks->source generation. Right click on "generateAvroJava" and run it. The Run item may not be available if indexing is still in progress, so wait for it to finish. The first run may take a while as it downloads jars etc.
  * Test the full build.
  * Open Run->Edit Configurations. Expand Defaults, and select Application. Add -XX:MaxPermSize=350m in VM options. Similarly add it to VM parameters for ScalaTest.
  * Try Run->Run... on a test like SparkSQLTest. (TODO: this still fails like above)


If sources and docs were selected during initial import, then it can take a really long time to get sources+docs for all dependencies. Instead one way could be to get the sources+docs for only scala-lang jars. The project setup after import already links sources and javadocs to appropriate locations in .m2 local cache, but since sources+docs were not selected during import so Maven may not have downloaded them yet. Check if you already have sources in m2 cache by opening a scala-lang class like Seq (hit Shift->Ctrl->T when using eclipse bindings and type scala.collection.Seq) and check if sources+docs are correctly shown. If not, then to easily download for selected jars do this:
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

