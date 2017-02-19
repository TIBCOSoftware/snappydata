## Example for Multiple-Host Configuration

Let's say you want to:

* Start two Locators (on node-a:9999 and node-b:8888), two servers (node-c and node-c) and a lead (node-l).

* Change the Spark UI port from 4040 to 9090. 

* Set spark.executor.cores as 10 on all servers. 

The following can be your conf files. 

```bash
$ cat conf/locators
node-a -peer-discovery-port=9999 -dir=/node-a/locator1 -heap-size=1024m -locators=node-b:8888
node-b -peer-discovery-port=8888 -dir=/node-b/locator2 -heap-size=1024m -locators=node-a:9999

$ cat conf/servers
node-c -dir=/node-c/server1 -heap-size=4096m -locators=node-b:8888,node-a:9999
node-c -dir=/node-c/server2 -heap-size=4096m -locators=node-b:8888,node-a:9999

$ cat conf/leads
# This goes to the default directory 
node-l -heap-size=4096m -J-XX:MaxPermSize=512m -spark.ui.port=9090 -locators=node-b:8888,node-a:9999 -spark.executor.cores=10
```
!!! Note
	Conf files are consulted when servers are started and also when they are stopped. So, we do not recommend changing the conf files while the cluster is running. </Note>

