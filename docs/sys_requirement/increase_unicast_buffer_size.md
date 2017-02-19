##Increase Unicast Buffer Size on Linux Platforms
On Linux platforms, execute the following commands as the root user to increase the unicast buffer size:

1. Edit the /etc/sysctl.conf file to include the following lines:</br>
 ```
 net.core.rmem_max=1048576
 net.core.wmem_max=1048576
 ```

2. Reload sysctl.conf:</br>
 ```
 sysctl -p
 ```

