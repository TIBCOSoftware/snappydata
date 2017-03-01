###<question> **Increase memory for Spark executors**</question></br>
<solution>
While starting servers using ./sbin/snappy-start-all.sh, you can provide arguments to each serve by modifying the conf/server file. For each server please provide the heap memory using  -heap-size=Xg
So, the entries in the conf/server would be like following:

machine_name  -heap-size=Xg 

Each line in the conf/server represents conf for that server.

For more on configurations, please follow the link http://snappydatainc.github.io/snappydata/configuration/
</solution>
