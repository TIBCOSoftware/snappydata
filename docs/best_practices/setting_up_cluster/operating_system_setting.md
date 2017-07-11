<a id="os_setting"></a>
##  Operating System Settings 

For best performance, the following operating system settings are recommended on the lead and server nodes.

**Ulimit** </br> 
Spark and SnappyData spawn a number of threads and sockets for concurrent/parallel processing so the server and lead node machines may need to be configured for higher limits of open files and threads/processes. </br>
</br>A minimum of 8192 is recommended for open file descriptors limit and nproc limit to be greater than 128K. 
</br>To change the limits of these settings for a user, the /etc/security/limits.conf file needs to be updated. A typical limits.conf used for SnappyData servers and leads looks like: 

```
ec2-user          hard    nofile      163840 
ec2-user          soft    nofile      16384
ec2-user          hard    nproc       unlimited
ec2-user          soft    nproc       524288
ec2-user          hard    sigpending  unlimited
ec2-user          soft    sigpending  524288
```
* `ec2-user` is the user running SnappyData.	


**OS Cache Size**</br> 
When there is lot of disk activity especially during table joins and during eviction, the process may experience GC pauses. To avoid such situations, it is recommended to reduce the OS cache size by specifying a lower dirty ratio and less expiry time of the dirty pages.</br> 
The following are the typical configuration to be done on the machines that are running SnappyData processes. 

```
sudo sysctl -w vm.dirty_background_ratio=2
sudo sysctl -w vm.dirty_ratio=4
sudo sysctl -w vm.dirty_expire_centisecs=2000
sudo sysctl -w vm.dirty_writeback_centisecs=300
```

**Swap File** </br> 
Since modern operating systems perform lazy allocation, it has been observed that despite setting `-Xmx` and `-Xms` settings, at runtime, the operating system may fail to allocate new pages to the JVM. This can result in process going down.</br>
It is recommended to set swap space on your system using the following commands.

```
# sets a swap space of 32 GB
sudo dd if=/dev/zero of=/var/swapfile.1 bs=1M count=32768
sudo chmod 600 /var/swapfile.1
sudo mkswap /var/swapfile.1
sudo swapon /var/swapfile.1
```