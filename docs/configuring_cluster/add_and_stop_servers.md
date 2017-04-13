## Add Servers to the Cluster and Stop Servers
SnappyData manages data in a flexible way that enables you to expand or contract your cluster at runtime to support different loads. To dynamically add more capacity to a cluster, you add new server members and specify the -`rebalance` option.

1. Open a new terminal or command prompt window, and create a directory for the new server:

        $ cd ~
        $ mkdir server3        

2. When you add a new server to the cluster, you can specify the -rebalance option to move partitioned table data buckets between host members as needed to establish the best balance of data across the distributed system. (See Rebalancing Partitioned Data on SnappyData Members for more information.) Start the new server to see rebalancing in action:

        $ snappy rowstore server start -dir=$HOME/server3 -locators=localhost[10101] -client-port=1530 -enable-network-partition-detection=true -rebalance
        Starting SnappyData Server using locators for peer discovery: localhost[10101]
        Starting network server for SnappyData Server at address localhost/127.0.0.1[1530]
        Logs generated in /home/gpadmin/server3/snappyserver.log
        SnappyData Server pid: 41165 status: running
          Distributed system now has 4 members.
          Other members: 192.168.125.147(39381:locator)<v0>:50344, 192.168.125.147(40612:datastore)<v5>:11337, 192.168.125.147(40776:datastore)<v6>:31019

3. View the contents of the new SnappyData directory:

        $ ls server3
        BACKUPGFXD-DEFAULT-DISKSTORE_1.crf  DRLK_IFGFXD-DEFAULT-DISKSTORE.lk
        BACKUPGFXD-DEFAULT-DISKSTORE_1.drf  snappyserver.log
        BACKUPGFXD-DEFAULT-DISKSTORE.if     start_snappyserver.log
        datadictionary

4. You can view all members of the distributed system using `snappy.` Return to the `snappy` session window and execute the query:

        snappy> select id from sys.members;
        ID                                                                             
        -------------------------------------------------------------------------------
        192.168.125.147(40612)<v5>:11337                                               
        192.168.125.147(39381)<v0>:50344                                               
        192.168.125.147(40776)<v6>:31019                                               
        192.168.125.147(41165)<v7>:35662                                               
        
        4 rows selected

5. Verify that all servers now host the data:

        snappy> select distinct dsid() as id from flights;
        ID                                                                             
        -------------------------------------------------------------------------------
        192.168.125.147(40612)<v5>:11337                                               
        192.168.125.147(40776)<v6>:31019                                               
        192.168.125.147(41165)<v7>:35662                                               
        
        3 rows selected

6. Examine the table data that each server hosts:

        snappy> select count(*) memberRowCount, dsid() from flights group by dsid();
        MEMBERROWCOUNT|2                                                               
        -------------------------------------------------------------------------------
        150           |192.168.125.147(40612)<v5>:11337                                
        114           |192.168.125.147(40776)<v6>:31019                                
        278           |192.168.125.147(41165)<v7>:35662                                
        
        3 rows selected

7. Exit the snappy session:

        snappy> exit;

8. You can stop an individual SnappyData server by using the `snappy rowstore server stop` command and specifying the server directory. To shut down all data stores at once, use the `snappy shut-down-all` command:

        $ snappy shut-down-all -locators=localhost[10101]
        Connecting to distributed system: locators=localhost[10101]
        Successfully shut down 3 members

9. After all data stores have stopped, shut down the locator as well:

        $ snappy locator stop -dir=$HOME/locator
        The SnappyData Locator has stopped.
