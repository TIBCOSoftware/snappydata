# DEPLOY PACKAGES

Deploys package in TIBCO ComputeDB.

## Syntax 

```pre
deploy package <unique-alias-name> ‘packages’ [ repos ‘repositories’ ] [ path 'some path to cache resolved jars' ]
```
*	**unique-alias-name** - A name to identify a package. This name can be used to remove the package from the cluster.  You can use alphabets, numbers, and underscores to create the name.

*	**packages** - Comma-delimited string of maven packages. 

*	**repos** - Comma-delimited string of remote repositories other than the **Maven Central** and **spark-package** repositories. These two repositories are searched by default.  The format specified by Maven is: **groupId:artifactId:version**.

*	**path** - The path to the local repository. This path should be visible to all the lead nodes of the system. If this path is not specified then the system uses the path set in the **ivy.home** property. if even that is not specified, the **.ivy2** in the user’s home directory is used.

## Description

Packages can be deployed in TIBCO ComputeDB using the **DEPLOY PACKAGE** SQL. You can pass the following through this SQL:

*	Name of the package.
*	Repository where the package is located.
*	Path to a local cache of jars.

!!!Note
	TIBCO ComputeDB requires internet connectivity to connect to repositories which are hosted outside the network. Otherwise the resolution of the package fails.

For resolving the package, Maven Central and Spark packages, located at http://dl.bintray.com/spark-packages, are searched by default. Hence, you must specify the repository only if the package is not there at **Maven Central** or in the **spark-package** repository.

!!!Tip
	Use **spark-packages.org** to search for Spark packages. Most of the popular Spark packages are listed here.

## Example 

*	Deploy packages from a default repository.
	
```
deploy package spark_deep_learning_0_3_0 'databricks:spark-deep-learning:0.3.0-spark2.2-s_2.11' path '/home/snappydata/work'
```

```pre
deploy package spark_redshift_300 'com.databricks:spark-redshift_2.10:3.0.0-preview1' path '/home/snappydata/work'
```

Deploy packages from a non-default repository.

**Related Topics**</br>

* [DEPLOY JAR](deploy_jar.md)
