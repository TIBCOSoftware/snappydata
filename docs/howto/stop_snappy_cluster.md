<a id="howto-stopcluster"></a>
# How to Stop a SnappyData Cluster

You can stop the cluster using the `sbin/snappy-stop-all.sh` command:

```bash
$ sbin/snappy-stop-all.sh
The SnappyData Leader has stopped.
The SnappyData Server has stopped.
The SnappyData Locator has stopped.
```
!!! Note:
	Ensure that all write operations on column table have finished execution when you stop a cluster, else, it can lead to the possible occurrence of a partial write.
