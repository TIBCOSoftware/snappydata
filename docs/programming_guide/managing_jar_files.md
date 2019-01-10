# Managing JAR Files

SnappyData provides system procedures that you can use to install and manage JAR files from a client connection. These can be used to install your custom code (for example code shared across multiple jobs) in SnappyData cluster.

## Installing a JAR

Related jobs may require some common libraries. These libraries can be made available to jobs by installing them. Use the SQLJ.INSTALL_JAR procedure to install a JAR file as mentioned below:

**Syntax:**

```pre
SQLJ.INSTALL_JAR(IN JAR_FILE_PATH VARCHAR(32672), IN QUALIFIED_JAR_NAME VARCHAR(32672), IN DEPLOY INTEGER)
```

* JAR_FILE_PATH  is the full path to the JAR file. This path must be accessible to the server on which the INSTALL_JAR procedure is being executed. If the JDBC client connection on which this procedure is being executed is using a locator to connect to the cluster, then actual client connection could be with any available servers. In this case, the JAR file path should be available to all servers

* QUALIFIED_JAR_NAME: The SnappyData name of the JAR file, qualified by a valid schema name.

* DEPLOY: This argument is currently ignored.

**Example:**

```pre
snappy> call sqlj.install_jar('/path_to_jar/procs.jar', 'APP.custom_procs', 0);
```

## Replacing a JAR

Use  SQLJ.REPLACE_JAR procedure to replace an installed JAR file

**Syntax:**

```pre
SQLJ.REPLACE_JAR(IN JAR_FILE_PATH VARCHAR(32672), IN QUALIFIED_JAR_NAME VARCHAR(32672))
```
* JAR_FILE_PATH  is the full path to the JAR file. This path must be accessible to the server on which the INSTALL_JAR procedure is being executed. If the JDBC client connection on which this procedure is being executed is using the locator to connect to the cluster, then actual client connection could be with any available servers. In this case, the JAR file path should be available to all servers.

* QUALIFIED_JAR_NAME: The SnappyData name of the JAR file, qualified by a valid schema name.

**Example:**

```pre
CALL sqlj.replace_jar('/path_to_jar/newprocs.jar', 'APP.custom_procs')
```

## Removing a JAR

Use SQLJ.REMOVE_JAR  procedure to remove a JAR file

**Syntax:**
```pre
SQLJ.REMOVE_JAR(IN QUALIFIED_JAR_NAME VARCHAR(32672), IN UNDEPLOY INTEGER)
```
* QUALIFIED_JAR_NAME: The SnappyData name of the JAR file, qualified by a valid schema name.

* UNDEPLOY: This argument is currently ignored.

**Example:**

```pre
CALL SQLJ.REMOVE_JAR('APP.custom_procs', 0)
```
