<a id="howto-stopcluster"></a>
# How to Stop a SnappyData Cluster

## Stopping the Cluster
You can stop the cluster using the `./sbin/snappy-stop-all.sh` command:

```pre
$ ./sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```
!!! Note
	Ensure that all write operations on column table have finished execution when you stop a cluster, else it can lead to a partial write.
    
## Stopping Individual Components

Instead of stopping the SnappyData cluster using the `snappy-stop-all.sh` script, individual components can be stopped on a system locally using the following commands:

!!! Tip
	All [configuration parameters](../configuring_cluster/configuring_cluster.md) are provided as command line arguments rather than reading from a configuration file.

```
$ ./bin/snappy locator stop -dir=/node-a/locator1
$ ./bin/snappy server stop -dir=/node-b/server1
$ ./bin/snappy leader stop -dir=/node-c/lead1
```

<!---## Stopping Individual Components in a Cluster

SnappyData recommends to stop the cluster using the `./sbin/snappy-stop-all.sh` command. However, you can stop individual components in a cluster. 

To stop individual components, do the following:

1.	Back up the original **conf/servers**, **conf/leads**, **conf/locators** files.
2.	Stop any of the cluster components.

	To stop the **server**, modify the **conf/servers** file to include only those node configurations which needs to be stopped and then execute `./snappy-servers.sh stop`. In the following example, the configuration for **host2** is commented in the **conf/servers **file and then the `./snappy-servers.sh stop` is executed. This will stop the server process on **host1**, running in the **server1** directory.

		original conf/servers file:
        host1 -dir=/home/snappy/server1 -heap-size=1G
        host2 -dir=/home/snappy/server2 -heap-size=1G

        Modified conf/servers file:
        host1 -dir=/home/snappy/server1 -heap-size=1G
        #host2 -dir=/home/snappy/server2 -heap-size=1G

         ./snappy-servers.sh stop
       
	    
    To stop the **lead**, modify the **conf/leads** file to include only those node configurations which needs to be stopped and then execute `./snappy-leads.sh stop`. In the following example **host1** is commented in the **conf/leads** file and then the `./snappy-leads.sh stop` is executed. This will stop the lead process on host4, running in the **lead2** directory.
    
   		original conf/leads:
        host1 -dir=/home/snappy/lead1
        host4 -dir=/home/snappy/lead2

        Modified conf/leads:
        #host1 -dir=/home/snappy/lead1
        host4 -dir=/home/snappy/lead2

        ./snappy-leads.sh stop

	To stop the **locator**, modify the **conf/locators** file to include only those node configuration which needs to be stopped and then execute `./snappy-locators.sh stop`. In the following example **host1** is commented in the **conf/locators** file and then the `./snappy-locators.sh stop` is executed. This will stop the locator process on host6, running in the **lcoator2** directory.
    
    	original conf/locators:
        host1 -dir=/home/snappy/locator1
        host6 -dir=/home/snappy/locator2

        Modified conf/locators:
        #host1 -dir=/home/snappy/locator1
        host6 -dir=/home/snappy/locator2

         ./snappy-locators.sh stop--->


