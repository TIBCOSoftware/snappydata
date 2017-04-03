# Setting up Cluster with SnappyData Docker Image
##Prerequisites

Before you begin, ensure that:
* You have Docker installed, configured and it runs successfully on your machine. Refer to the [Docker documentation](http://docs.docker.com/installation) for more information on installing Docker.
* The Docker containers have access to at least 4GB of RAM on your machine.
* To run Docker you may need to modify the RAM used by the virtual machine which is running the Docker daemon. Refer to the Docker documentation for more information.

1. **Verify that your installation is working correctly**

	Run the following command on your machine:
 	```
 	$ docker run hello-world
 	```

2. **Start a basic cluster with one data node, one lead and one locator**

 	**For Linux**

 	```
 	$ docker run -itd --net=host --name snappydata snappydatainc/snappydata start all
 	```
    
 	**For Mac OS**

 	If you are using MAC OS you need to redirect the ports manually.
 	!!! Note: 
    	If you use `--net=host`, it may not work correctly on the Mac OS.

 	Run the following command to start SnappyData Cluster on Mac OS:

 	```
 	docker run -d --name=snappydata -p 4040:4040 -p 1527:1527 -p 1528:1528 snappydatainc/snappydata start all -J-Dgemfirexd.hostname-for-clients=<Machine_IP/Public_IP>
 	```
    
 	The `-J-Dgemfirexd.hostname-for-clients` parameter sets the IP Address or Host name that this server listens on for client connections.

 	!!! Note
    	It may take some time for this command to complete execution.</Note>

3. **Check the Docker Process**

 	```
 	$ docker ps -a
	```

4. **Check the Docker Logs**

 	Run the following command to view the logs of the container process.
	 
	```
 	$ docker logs snappydata
 	starting sshd service
 	Starting sshd:
  	[ OK ]
 	Starting SnappyData Locator using peer discovery on: localhost[10334]
 	Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
 	Logs generated in /opt/snappydata/work/localhost-locator-1/snappylocator.log
     SnappyData Locator pid: 110 status: running
     Starting SnappyData Server using locators for peer discovery: localhost:10334
     Starting DRDA server for SnappyData at address localhost/127.0.0.1[1527]
     Logs generated in /opt/snappydata/work/localhost-server-1/snappyserver.log
     SnappyData Server pid: 266 status: running
     Distributed system now has 2 members.
     Other members: localhost(110:locator)<v0>:63369
     Starting SnappyData Leader using locators for peer discovery: localhost:10334
     Logs generated in /opt/snappydata/work/localhost-lead-1/snappyleader.log
     SnappyData Leader pid: 440 status: running
     Distributed system now has 3 members.
     Other members: 192.168.1.130(266:datastore)<v1>:47290, localhost(110:locator)<v0>:63369
 	```
 	
    The query results display *Distributed system now has 3 members*.

5. **Connect SnappyData with the Command Line Client**

	```
	$ docker exec -it snappydata ./bin/snappy-shell
	```
 
6. **Connect Client on port “1527”**

	```
 	$ snappy> connect client 'localhost:1527';
	```

7. **View Connections**

	```
 	snappy> show connections;
 	CONNECTION0* -
  	jdbc:gemfirexd://localhost[1527]/
 	* = current connection
 	```
    
8. **Check Member Status**

 	```
 	snappy> show members;
 	```

9. **Stop the Cluster**

 	```
 	$ docker exec -it snappydata ./sbin/snappy-stop-all.sh
 	The SnappyData Leader has stopped.
 	The SnappyData Server has stopped.
 	The SnappyData Locator has stopped.
 	```

10. **Stop SnappyData Container**

 	```
 	$ docker stop snappydata
 	```