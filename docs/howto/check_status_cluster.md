<a id="howto-statuscluster"></a>
# How to check the status of the SnappyData Cluster
You can check the status of a running cluster using the following command:


```bash
$ sbin/snappy-status-all.sh
SnappyData Locator pid: 9368 status: running
SnappyData Server pid: 9519 status: running
  Distributed system now has 2 members.
  Other members: localhost(9368:locator)<v0>:16944
SnappyData Leader pid: 9699 status: running
  Distributed system now has 3 members.
  Other members: localhost(9368:locator)<v0>:16944, 192.168.63.1(9519:datastore)<v1>:46966
```

You can check the SnappyData UI by opening `http://<leadHostname>:5050` in your browser, where `<leadHostname>` is the host name of your lead node. Use [Snappy SQL shell](use_snappy_shell.md) to connect to the cluster and perform various SQL operations.
