There were few proposals about how to manage the various repositories mentioned in [this document](https://docs.google.com/document/d/1jC8z-WPzK0B8J6p3jverumK4gcbprmFiciXYKd2JUVE/edit#). Based on few discussions, we shortlisted Proposal 4 in the document. 

According to "Proposal 4" gemxd and snappy-spark repositories will be independent of any other repository. There will be a third repository that will hold the code of Snappy - snappy-commons. Snappy-Commons will have two projects: 
 
(a) **snappy-core** - Any code that is an extension to Spark code and is not dependent on gemxd, job server etc. should go in here. For e.g. SnappyContext, cluster manager etc. 
(b) **snappy-tools** - This is the code that serves as the bridge between GemXD and snappy-spark.  For e.g. query routing, job server initialization etc. 

Code in snappy-tools can depend on snappy-core but it cannot happen other way round. 

For building these two projects, we are using sbt's feature of multi project builds. We have a build.sbt file at the root project that is snappy-commons. This build.sbt file drives the build of both the projects. We have aliases for the projects as root, core and tools. 

On sbt shell, if compile is fired, it compiles root, core and tools. To build an individual project, on sbt shell, you can write - "project core" and then fire compile.  
  
'''
../snappy-commons>sbt 
[info] Loading project definition from /hemantb1/snappy/repos/snappy-commons/project
[info] Set current project to root (in build file:/hemantb1/snappy/repos/snappy-commons/)
> compile 
[info] Updating {file:/hemantb1/snappy/repos/snappy-commons/}root...
[info] Resolving org.fusesource.jansi#jansi;1.4 ...
[info] Done updating.
[info] Compiling 53 Scala sources and 15 Java sources to /hemantb1/snappy/repos/snappy-commons/snappy-core/target/scala-2.10/classes...
[success] Total time: 22 s, completed Sep 23, 2015 5:28:17 PM
> project core 
[info] Set current project to snappy-core (in build file:/hemantb1/snappy/repos/snappy-commons/)
> compile 
[success] Total time: 0 s, completed Sep 23, 2015 5:29:19 PM
> project root 
[info] Set current project to root (in build file:/hemantb1/snappy/repos/snappy-commons/)
> compile 
[success] Total time: 0 s, completed Sep 23, 2015 5:29:26 PM
'''
> 
