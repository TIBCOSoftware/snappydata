# DEPLOY JARS

Deploys a jar in a running system.

## Syntax

```pre
deploy jar <unique-alias-name> ‘jars’
```
*	**unique-alias-name** - A name to identify the jar. This name can be used to remove the jar from the cluster.  You can use alphabets, numbers and underscores to create the name.

*	**jars** - Comma-delimited string of jar paths. These paths are expected to be accessible from all the lead nodes in SnappyData.

## Description

SnappyData provides a method to deploy a jar in a running system through SQL. You can execute the **deploy jar ** command to deploy dependency jars. In cases where the artifacts of the dependencies are not available in the provided cache path, then during restart, it automatically resolves all the packages and jars again and installs them in the system.

## Example 

```
deploy jar SparkDaria spark-daria_2.11.8-2.2.0_0.10.0.jar
```



**Related Topics**</br>

* [DEPLOY PACKAGE](deploy_package.md)
* [UNDEPLOY](undeploy.md)

