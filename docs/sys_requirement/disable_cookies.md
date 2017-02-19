<a id="syn-cookies"></a>
##Disable SYN Cookies on Linux Platforms
Many default Linux installations use SYN cookies to protect the system against malicious attacks that flood TCP SYN packets. The use of SYN cookies dramatically reduces network bandwidth, and can be triggered by a running SnappyData distributed system.

If your SnappyData distributed system is otherwise protected against such attacks, disable SYN cookies to ensure that SnappyData network throughput is not affected.

To disable SYN cookies permanently:

1. Edit the /etc/sysctl.conf file to include the following line: </br>
 ```
 net.ipv4.tcp_syncookies = 0
 ```
 </br>Setting this value to zero disables SYN cookies.

2. Reload sysctl.conf: </br>
 ```
 sysctl -p
 ```
 
