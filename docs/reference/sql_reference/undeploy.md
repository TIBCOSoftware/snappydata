# UNDEPLOY 

Removes the jars that are directly installed and the jars that are associated with a package, from the system.


## Syntax 

```pre
undeploy <unique-alias-name>;
```
*	**jar-name** - Name of the jar that must be removed.  


## Description

The command removes the jars that are directly installed and the jars that are associated with a package, from the system. 

!!!Note
	The removal is only captured when you use the **undeploy** command,  the jars are removed only when the system restarts.


## Example 

	
```
undeploy spark_deep_learning_0_3_0; 
```



**Related Topics**</br>

* [DEPLOY PACKAGE](deploy_package.md)
* [DEPLOY JAR](deploy_jar.md)

