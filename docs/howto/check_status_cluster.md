<a id="howto-statuscluster"></a>
# How to Check the Status of the TIBCO ComputeDB™ Cluster
You can check the status of a running cluster using the following command:


```pre
$ ./sbin/snappy-status-all.sh
SnappyData Locator pid: 9748 status: running
SnappyData Server pid: 9887 status: running
SnappyData Leader pid: 10468 status: running
```

You can check the TIBCO ComputeDB UI by opening `http://<leadHostname>:5050` in your browser, where `<leadHostname>` is the host name of your lead node. Use [Snappy SQL shell](use_snappy_shell.md) to connect to the cluster and perform various SQL operations.

**Related Topics**

* [TIBCO ComputeDB UI](../monitoring/monitoring.md)
