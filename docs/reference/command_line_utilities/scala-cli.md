# snappy-scala CLI

The snappy-scala CLI, which is similar to spark-shell in its capabilities, is introduced as an experimental feature in this release. snappy-scala CLI is built on top of the **exec scala** feature and the already existing snappy CLI utility. This is not a true Scala interpreter but mimics a scala or a spark-shell kind of interpreter. Here, each line of code is shipped to the lead node. The code is interpreted on the lead node. The result is brought back to the snappy-scala shell.

Features such as auto-complete and full-fledged colon command of a scala shell are not as stable as in the spark-shell. This is because in Apache Spark, the spark-shell is itself the Application driver VM whereas in TIBCO ComputeDB, the driver is the lead node, which is a remote process from the command-line utility point of view. The snappy-scala CLI utility connects like a client utility and therefore has the limitation mentioned above. Those limitations can be overcome and will be considered in a future release.

### Command-line options

```
$ ./bin/snappy-scala -h

Usage:

snappy-scala [OPTIONS]

OPTIONS and Default values

   -c LOCATOR_OR_SERVER_ENDPOINT  (default value is localhost:1527)

   -u USERNAME                    (default value is APP)

   -p PASSWORD                    (default value is APP)

   -r SCALA_FILE_PATHS            (comma separated paths if multiple)

   -h, --help                     (prints script usage)

```


As seen above there are 5

### Securing the Usage of snappy-scala CLI

This is the same as securing the usage of **exec scala**. For more details refer to [Securing the Usage of exec scala SQL](/programming_guide/scala_interpreter.md#secureexscala). 

