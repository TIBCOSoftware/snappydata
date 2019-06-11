# list-missing-disk-stores

Lists all disk stores with the most recent data for which other members are waiting.

## Syntax

**Secured cluster**

```
./bin/snappy list-missing-disk-stores -locators=<addresses> -auth-provider=<auth-provider> -user=<username> -password=<password> -gemfirexd.auth-ldap-server=ldap://<ldap-server-host>:<ldap-server-port>/  -gemfirexd.auth-ldap-search-base=<search-base-values> -gemfirexd.auth-ldap-search-dn=<search-dn-values> -gemfirexd.auth-ldap-search-pw=<password>
```
**Non-secured cluster**

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:bind address
```
If no locator option is specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

The table describes options for snappy list-missing-disk-stores.

|Option|Description|
|-|-|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other SnappyData distributed system property.|
|Authentication properties| Refer [Authentication Properites](/security/launching_the_cluster_in_secure_mode.md#authproperties).|


## Example

**Secured cluster**

```
./bin/snappy list-missing-disk-stores -locators=localhost:10334 -auth-provider=LDAP -user=snappy1 -password=snappy1  -J-Dgemfirexd.auth-ldap-server=ldap://localhost:389/ -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=user123

Connecting to distributed system: locators=localhost[10334]
18/11/15 18:08:26.802 IST main<tid=0x1> INFO SNAPPY: TraceAuthentication: Enabling authorization for auth provider LDAP
18/11/15 18:08:27.575 IST main<tid=0x1> INFO SnappyUnifiedMemoryManager: BootTimeMemoryManager org.apache.spark.memory.SnappyUnifiedMemoryManager@16943e88 configuration:
		Total Usable Heap = 786.2 MB (824374722)
		Storage Pool = 393.1 MB (412187361)
		Execution Pool = 393.1 MB (412187361)
		Max Storage Pool Size = 628.9 MB (659499777)
The distributed system did not have any missing disk stores
```

**Non-secured Cluster**

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334

Connecting to distributed system: locators=localhost[10334]
log4j:WARN No appenders could be found for logger (com.gemstone.org.jgroups.util.GemFireTracer).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/11/19 14:08:33 INFO SnappyUnifiedMemoryManager: BootTimeMemoryManager org.apache.spark.memory.SnappyUnifiedMemoryManager@33a053d configuration:
		Total Usable Heap = 786.2 MB (824374722)
		Storage Pool = 393.1 MB (412187361)
		Execution Pool = 393.1 MB (412187361)
		Max Storage Pool Size = 628.9 MB (659499777)
The distributed system did not have any missing disk stores

```

