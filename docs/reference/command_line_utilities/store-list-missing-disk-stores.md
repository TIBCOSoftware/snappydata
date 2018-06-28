# list-missing-disk-stores

Lists all disk stores with the most recent data for which other members are waiting.

## Syntax

```pre
./bin/snappy list-missing-disk-stores 
   <-locators=<addresses>> [-bind-address=<address>] [-<prop-name>=<prop-value>]*
```

If no locator option is specified on the command-line, the command uses the gemfirexd.properties file (if available) to determine the distributed system to which it should connect.

The table describes options for snappy list-missing-disk-stores.

|Option|Description|
|-|-|
|-locators|List of locators used to discover members of the distributed system. Supply all locators as comma-separated host:port values. The port is the `peer-discovery-port` used when starting the cluster (default 10334). This is a mandatory field.|
|-bind-address|The address to which this peer binds for receiving peer-to-peer messages. By default SnappyData uses the hostname, or localhost if the hostname points to a local loopback address.|
|-prop-name|Any other SnappyData distributed system property.|


## Example

```pre
./bin/snappy list-missing-disk-stores -locators=localhost:10334
Connecting to distributed system: -locators=localhost:10334
The distributed system did not have any missing disk stores
```
