# Using java.util.logging.Logger for Application Log Messages

Applications that use the SnappyData JDBC peer driver can log messages through the java.util.Logger logging API. You can obtain a handle to the java.util.Logger logger object by entering `com.pivotal.gemfirexd` after the application connects to the SnappyData cluster with the peer driver.

<a id="example"></a>
For example:

```pre
import java.util.logging.Logger;
 Logger logger = Logger.getLogger("com.pivotal.gemfirexd");
 logger.info("Connected to a SnappyData system");
```


