# Using Multiple Containers with Docker Compose

Download and install the latest version of Docker compose. Refer to the [Docker documentation](https://docs.docker.com/compose/install/) for more information.

1. **Verify the Installation**

	Check the version of Docker Compose to verify the installation.
    
    ```
 	$ docker-compose -v
 	docker-compose version 1.8.1, build 878cff1
    ```
 	
2. **Set an environment variable called External_IP**
	
    ```
 	export EXTERNAL_IP=<your machine ip>
    ```
 	 
3. **Use the compose file (docker-compose.yml) file to run Docker Compose**

 	Download the [docker-compose.yml](https://raw.githubusercontent.com/SnappyDataInc/snappy-cloud-tools/master/docker/docker-compose.yml) file, and then run it from the downloaded location using the following command:

 	```
 	$ docker-compose -f docker-compose.yml up -d
 	Creating network "docker_default" with the default driver
 	Creating locator1
 	Creating server1
 	Creating snappy-lead1
 	```

	This creates three containers; a locator, server and lead. 

 		
		$ docker-compose ps
		Name                  Command               State                        Ports                       
		--------------------------------------------------------------------------------------------------------
		locator1       start locator                    Up      0.0.0.0:10334->10334/tcp, 0.0.0.0:1527->1527/tcp 
		server1        bash -c sleep 10 && start  ...   Up      0.0.0.0:1528->1528/tcp                           
		snappy_lead1   bash -c sleep 20 && start  ...   Up      0.0.0.0:4040->4040/tcp                           
		

4. **View the logs**

     Run the following command to view the logs and to verify the services running inside the Docker Compose.
 		
 		$ docker-compose logs
 		Attaching to snappy-lead1, server1, locator1
 		server1       | Starting SnappyData Server using locators for peer discovery: locator1:10334
 		server1       | Starting DRDA server for SnappyData at address server1/172.18.0.3[1528]
 		snappy-lead1  | Starting SnappyData Leader using locators for peer discovery: locator1:10334
 		server1       | Logs generated in /opt/snappydata/work/localhost-server-1/snappyserver.log
 		snappy-lead1  | Logs generated in /opt/snappydata/work/localhost-lead-1/snappyleader.log
 		server1       | SnappyData Server pid: 83 status: running
 		snappy-lead1  | SnappyData Leader pid: 83 status: running
 		server1       |   Distributed system now has 2 members.
 		snappy-lead1  |   Distributed system now has 3 members.
 		snappy-lead1  |   Other members: docker_server1(83:datastore)<v1>:53707, locator1(87:locator)<v0>:44102
 		server1       |   Other members: locator1(87:locator)<v0>:44102
 		locator1      | Starting SnappyData Locator using peer discovery on: locator1[10334]
 		locator1      | Starting DRDA server for SnappyData at address locator1/172.18.0.2[1527]
 		locator1      | Logs generated in /opt/snappydata/work/localhost-locator-1/snappylocator.log
 		locator1      | SnappyData Locator pid: 87 status: running
 		```

	The above logs display that the cluster has started successfully on the three containers.

5. **Connect SnappyData with the Command Line Client**

	The following example illustrates how to connect with snappy-shell. 
 
 	[Download](https://github.com/SnappyDataInc/snappydata/releases/download/v0.8/snappydata-0.8-bin.tar.gz) the binary files from the SnappyData repository. Go the location of the **bin** directory in the SnappyData home directory, and then start the snappy-shell.

		bin$ ./snappy-shell
 		SnappyData version 0.8
 		snappy>
 		
	!!! Note
		You can also connect to SnappyData with DB client tools like dbSchema, DBVisualizer or Squirrel SQL client using the **snappydata-store-client-1.5.0.jar** file available on the official [SnappyData Release page](#https://github.com/SnappyDataInc/snappydata/releases). Refer to the documentation provided by your client tool for instructions on how to make a JDBC connection.
 
6. **Make a JDBC connection**

		 $ snappy> connect client '<Your Machine IP>:1527';
 		Using CONNECTION0
 		snappy>

 
7. ** List Members**
 		
 		snappy> show members;
 		ID                            |HOST                          |KIND                          |STATUS              |NETSERVERS                    |SERVERGROUPS
 		-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 		3796bf1ff482(135)<v0>:5840    |3796bf1ff482                  |locator(normal)               |RUNNING             |3796bf1ff482/172.18.0.2[1527] |
 		7b54228d4d02(131)<v1>:50185   |7b54228d4d02                  |datastore(normal)             |RUNNING             |192.168.1.130/172.18.0.3[1528]|
 		e847fed458a6(130)<v2>:35444   |e847fed458a6                  |accessor(normal)              |RUNNING             |                              |IMPLICIT_LEADER_SERVERGROUP
		
 		3 rows selected
 		snappy>
		

8. **View Connections**
		
 		snappy> show connections;
 		CONNECTION0* -
  		jdbc:gemfirexd://localhost[1528]/
 		* = current connection
 		
9. **Stop Docker Compose**

	To stop and remove containers from the Docker Engine, run the command:
		
 		$ docker-compose -f docker-compose.yml down
 		Stopping snappy_lead1 ... done
 		Stopping server1 ... done
 		Stopping locator1 ... done
 		Removing snappy_lead1 ... done
 		Removing server1 ... done
 		Removing locator1 ... done
 		Removing network dockercompose_snappydata
 
	!!! Note 
 		When you remove containers from the Docker engine, any data that exists on the containers is destroyed. 
