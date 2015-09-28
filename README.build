## Building using maven

Maven poms have been arranged in a way (with some shell scripting) so that all of snappy projects including snappy's spark variant can be built from the top-level:
  * The full build and Intellij import has been tested with only JDK7. If you are using JDK8, then you are on your own. On Ubuntu/Mint systems, best way to get Oracle JDK7 as default:

    - add webupd8 java repository: sudo add-apt-repository ppa:webupd8team/java
    - install and set jdk7 as default: sudo aptitude install oracle-java7-set-default
    - you can also install oracle-java7-unlimited-jce-policy package for enhanced JCE encryption
    - this will set java to point to JDK7 version and also set JAVA_HOME, so start a new shell for the changes to take effect; also run "source /etc/profile.d/jdk.sh" to update JAVA_HOME (or else you will need to logoff and login again for the JAVA_HOME setting to get applied)

  * Ensure that snappy-spark repository has been cloned in the same directory as snappy-commons as a sibling. Alternatively set the SPARK_HOME environment variable to point to the location of snappy-spark.
  * Test the build with: ./build/mvn clean compile
  * If you have not made any changes to snappy-spark and already built it once, then its much faster to just compile snappy-core and snappy-tools: ./build/mvn -pl snappy-core,snappy-tools compile. Or simply: ./build/mvn -skip-spark compile
  * Package jars: ./build/mvn package
  * Run a snappy-core test application: ./build/mvn -pl snappy-core exec:java -Dexec.mainClass=io.snappydata.app.SparkSQLTest


## Setting up Intellij

If the build works fine, then it is pretty straightforward to import all the projects into Intellij:
  * Select import project, then point to the snappy-commons directory
  * Use external Maven import. Select defaults, next, next ... finish
  * Once import finishes, copy codeStyleSettings.xml in snappy-commons to .idea directory created by Intellij
  * Expand Profiles in the "Maven Projects" tab on the right margin. Select "hadoop-2.4", "hive", "hive-thriftserver", "yarn" modules. Accept refresh maven import in the pop-up.
  * Click the second icon in the "Maven Projects" tab toolbar named "Generate Sources and Update Folders For All Projects"
  * Test the full build. If there are still any errors due to missing SparkFlumeProtocol in SparkAvroCallbackHandler and related classes, then select and right-click on the "Spark Project External Flume Sink" module in "Maven Projects" tab. Click on "Generate Sources and Update Folders" option.
  * Open Run->Edit Configurations. Expand Defaults, and select Application. Add "-XX:MaxPermSize=350m" in VM options. Similarly add it to VM parameters for ScalaTest.
  * Try Run->Run... on a test like SparkSQLTest.


If sources and docs were selected during initial import, then it can take a really long time to get sources+docs for all dependencies. Instead one way could be to get the sources+docs for only scala-lang jars. The project setup after import already links sources and javadocs to appropriate locations in .m2 local cache, but since sources+docs were not selected during import so Maven may not have downloaded them yet. Check if you already have sources in m2 cache by opening a scala-lang class like Seq (hit Shift->Ctrl->T when using eclipse bindings and type scala.collection.Seq) and check if sources+docs are correctly shown. If not, then to easily download for selected jars do this:
  * Open the File->Project Structure->Libraries
  * Click on the '+' sign at the top to add new library, and choose Maven.
  * In the box, provide "scala-library-2.10.4" and click on the search tool.
  * Select the "org.scala-lang:scala-library:2.10.4" in the drop down. Then check "Sources", "JavaDocs" options and go ahead.
  * Do the same for others like "scala-reflect-2.10.4" and "scala-compiler-2.10.4" as required.
  * Once this is done, don't select OK on the main Project Structure box. Instead hit "Cancel" and it should be all good since we only wanted to get Maven to download the sources and docs for these jars.
