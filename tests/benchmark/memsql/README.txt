How to run TPCH queries on Memsql cluster
 This is not gone be easy run. You have to first setup memsql on cluster and to be in directory which has all required files. Send mail at kbachhav@snappydat.io to get more details.
1> Create a file PerfRun.conf.
    A template file is provided in which all mandatory variables (like machine names, queries, output location, data size) are defined
2> Not completed 1_cleanMemsql.sh.
    Cleaning Memsql on different machines is not possible thorugh script. Hence do it manually form aggregator node
3> Run 2.Setup.sh.
    This script file starts aggregator and leaf nodes.
4> Run 3_createAndLoadTable.sh
     This script is used to create all TPCH related tables and load the data. This is actually a SnappySQLJob
5> Run 4_jobstatus.sh
         This is to check the status of loading of tables
6> Run 5_execute.sh
               Run this script to execute queries
7> Run 6_generateResult.sh
        This consolidate all performance related results in output location. Files are generated corresponding to each query which has timing for each iterations.
        Avg.out files collects the average time of last 2 iterations of each query.