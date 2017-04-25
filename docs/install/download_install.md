#Overview
(TO BE DONE)

!!! Note:  

	Configuring the limit for Open Files and Threads/Processes 

	On a Linux system you can set the limit of open files and thread processes in the **/etc/security/limits.conf** file. </br>A minimum of **8192** is recommended for open file descriptors limit and **>128K** is recommended for the number of active threads. </br>A typical configuration used for SnappyData servers and leads can look like:

			snappydata          hard    nofile      81920
			snappydata          soft    nofile      8192
			snappydata          hard    nproc       unlimited
			snappydata          soft    nproc       524288
			snappydata          hard    sigpending  unlimited
			snappydata          soft    sigpending  524288

	Here `snappydata` is the user name under which the SnappyData processes are started. 


