#!/usr/bin/env bash

source clusterUpgrade.conf
cnt=1
tableArr=(colTable rowPartitionedTable rowReplicatedTable)

if [ $# -lt 2 ]; then
   echo "ERROR: incorrect argument specified: " "$@"
   echo "The script should have atleast 2 builds for comparison"
   echo "Usage:./clusterUpgrade.sh <snappyBuildPath1> <snappyBuildPath2> ... <snappyBuildPathN>"
   exit 1
fi

totalNoBuilds=$(echo $#)
echo "The total no. of builds to be tested are $totalNoBuilds"

#Do clean up
finalResultDir=$resultDir/resultDir
rm -rf $finalResultDir/*
rm -rf $snappydataTestDir/tempConf/*

if [ ! -d "$finalResultDir" ]; then
   mkdir $finalResultDir
fi

 validate() {
   cd $finalResultDir
   fileNo=$(($2-1))
   echo "Comparing $1 with output_sorted_$fileNo.log"
   diff output_sorted_$fileNo.log $1
 }

for i in "$@";
  do
    if [ $cnt -gt 1 ]; then
      echo "========= Copying the previous conf from tempConf to $i/conf ==========="
      cp -r $snappydataTestDir/tempConf/* $i/conf/
    fi

    echo -e "\n\n============ Starting Snappy cluster =========="
    echo "For build $i cluster start status is in $finalResultDir/clusterStartStatus.log"
    echo -e "\n====== Build = $i ============" >> $finalResultDir/clusterStartStatus.log
    sh $i/sbin/snappy-start-all.sh >> $finalResultDir/clusterStartStatus.log
    sh $i/sbin/snappy-status-all.sh >> $finalResultDir/clusterStartStatus.log
    sleep 30
    echo "===================================================================="
    grep 'Exception\|Error\|WARN' ${finalResultDir}/clusterStartStatus.log
    if [ $? -eq 0 ]; then
       echo "Cluster start up encountered an Exception/Error/WARN"
       echo "Please see the logs"
       exit 1
    fi

    echo -e "\n\n============ Starting Spark cluster =========="
        sh $i/sbin/start-all.sh

    #execute create table script.
    if [ $cnt -eq 1 ]; then
      echo -e "\n\n=========== Loading table for the first time ========="
      sh $i/bin/snappy run -file=$createTableScript -client-bind-address=localhost -client-port=1527
      echo -e "\n=========== Finished loading tables ==========="
    fi

    ########### Validating the previous build's output file with the presently loaded cluster ###############
    if [ $cnt -gt 1 ]; then
      sh $i/bin/snappy run -file=$validationScript -client-bind-address=localhost -client-port=1527 >> $finalResultDir/outputBfrOps_$cnt.log
      sort $finalResultDir/outputBfrOps_$cnt.log > $finalResultDir/outputBfrOps_sorted_$cnt.log
      validate outputBfrOps_sorted_$cnt.log cnt
    fi

    sh $i/bin/snappy run -file=$dmlScript -client-bind-address=localhost -client-port=1527 > $finalResultDir/dmlOutput_$cnt.log

    echo -e "\n ========== Executing snappy job ============"
    $i/bin/snappy-job.sh submit --lead localhost:8090 --app-name myApp --class $snappyJobClassName --app-jar $jarFile --conf queryFile=$validationScript > $finalResultDir/jobRun.txt
    jobId=$(grep -r 'jobId' $finalResultDir/jobRun.txt|cut -d'"' -f4)
    $i/bin/snappy-job.sh status --lead localhost:8090 --job-id $jobId > $finalResultDir/jobStatus.txt
    while ! grep 'FINISHED\|ERROR' $finalResultDir/jobStatus.txt > /dev/null
    do
      sleep 5
      echo -e "\n Waiting for the job to finish"
      $i/bin/snappy-job.sh status --lead localhost:8090 --job-id $jobId > $finalResultDir/jobStatus.txt
    done
    cat $finalResultDir/jobStatus.txt

    echo -e "\n\n========= Execute Spark job ==========="
    sh $i/bin/spark-submit --class $sparkJobClassName --master spark://$HOSTNAME:7077  --executor-memory 1280m --conf snappydata.connection=localhost:1527 $jarFile $validationScript > "$finalResultDir/sparkJobOutPut_$cnt.log"

    sh $i/bin/snappy run -file=$validationScript -client-bind-address=localhost -client-port=1527 >> $finalResultDir/output_$cnt.log
    sort $finalResultDir/output_$cnt.log > $finalResultDir/output_sorted_$cnt.log

    echo -e "\n\n=========== Stopping Snappy cluster =========="
    sh $i/sbin/snappy-stop-all.sh >> $finalResultDir/clusterStopStatus.log
    sh $i/sbin/snappy-status-all.sh
    sleep 30
    echo -e "\n\n=========== Stopping Spark cluster =========="
    sh $i/sbin/stop-all.sh

    echo -e "\n========= Copying the present conf to $snappydataTestDir/tempConf ========"
    if [ ! -d "$snappydataTestDir/tempConf" ]; then
      mkdir $snappydataTestDir/tempConf
    fi
    cp -r $i/conf/* $snappydataTestDir/tempConf
    ((cnt++))
  done

  echo -e "\n\n=============Finished Cluster Upgradation test, results are at $finalResultDir =================="

