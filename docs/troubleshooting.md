#Troubleshooting and Analysis
###<question> **Getting thread dumps from GUI or detailed stacks using SIGURG**</question></br>
<solution>
Text
</solution>

###<question> **Observing logs for warning/severe/errors and decoding log messages**</question></br>
<solution>
Text
</solution>

###<question> **Collocating logs and stats using collect-debug-artifacts.sh**</question></br>
<solution>
Text
</solution>

###<question> **Using VSD (can we make use of GemFire VSD docs?)**</question></br>
<solution>
Text
</solution>

###<question> **Enabling/disabling logging levels/traces on-the-fly**</question></br>
<solution>
Text
</solution>

###<question> Cannot connect using EC2 Scripts</question></br>
<solution>
Text
</solution>

###<question> **Increase memory for Spark executors**</question></br>
<solution>
While starting servers using ./sbin/snappy-start-all.sh, you can provide arguments to each serve by modifying the conf/server file. For each server please provide the heap memory using  -heap-size=Xg
So, the entries in the conf/server would be like following:

machine_name  -heap-size=Xg 

Each line in the conf/server represents conf for that server.

For more on configurations, please follow the link http://snappydatainc.github.io/snappydata/configuration/
</solution>

###<question>**Can I read orc files from snappy-shell ?**
</question>

<solution> 
ORC is a supported format in Spark SQL.
```
create Table orcTable using orc options (path "path to orc file")
```

This would create a spark temporary table. You would need to explicitly load into a column table like - create table colTable using column options(..) as select * from orcTable ...
</solution>

###<question>**Analysing Error Messages**</question>

<question>**Error message "IllegalArgumentException: System memory" is reported.**
</question>

<solution> 
You need to specify the configuration files for Snappy components. See [configuration properties](#configuration/#configuration-files) for more infromation.

```
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
</solution>

<question>
**Snappy shell cannot connect to server when starting snappy-shell, and an error is reported, when executing the command to connect to the client**
</question>

<solution> 
The problem looks to be that the configuration is trying to start two locators both on same machine using same port. 

The first locator starts successfully, but the second locator fails because it is presumably on the same host/port. In this case, you probably have unintentially configured the first locator.

Subsequently, none of the servers/leads start because they are trying to connect to the second locator, but the one locator which was successful is running on localhost.

Check the conf/locators file and correct it.
</solution>


<question>
</question>
<solution> 
</solution>
