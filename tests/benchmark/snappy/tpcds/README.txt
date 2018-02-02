How to run TPCDS queries on snappy cluster
1> Create a file PerfRun.conf.
    A template file is provided in which all mandatory variables (like machine names, queries, spark properties,
    snappy location, output location, data size) are defined
2> Run 1_cleanJavaOnMachines.
    which stops locators, servers, leads defined. Also you can kill all java process running on these machines
    by uncommenting last few lines of this script file
3> Run 2_Setup.sh.
    This script file starts locator, server, leads. Spark properties defined in PerfRun.conf is used while staring lead.
    serverMemory defined in PerfRun.conf is used while starting servers.
    Jar files containing TPCDS Code is specified while starting locator,lead, servers
4> Run 3_createAndLoadTable.sh
     This script is used to create all TPCDS related tables and load the data. This is actually a SnappySQLJob
5> Run 4_6_jobStatus.sh
      This is to check the status of above createTable job. MAke sure that above createTable job is finished
6> Run 5_execute.sh
      Run this script only when 3_createTable job is finished. This script executes the queries specified
7> Run 4_6_jobStatus.sh
         This is to check the status of above execute query job. Make sure that above createTable job is finished
