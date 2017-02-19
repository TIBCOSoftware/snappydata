## Spark Specific Properties 

Since SnappyData embeds Spark components, [Spark Runtime environment properties](http://spark.apache.org/docs/latest/configuration.html#runtime-environment) (like  spark.driver.memory, spark.executor.memory, spark.driver.extraJavaOptions, spark.executorEnv) do not take effect. They have to be specified using SnappyData configuration properties. 

Apart from these properties, other Spark properties can be specified in the configuration file of the Lead nodes. You have to prefix them with a _hyphen(-)_. The Spark properties that are specified on the Lead node are sent to the Server nodes. Any Spark property that is specified in the conf/servers or conf/locators file is ignored. 

!!! Note
	Currently we do not honor properties specified using spark-config.sh. </Note>

