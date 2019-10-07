#!/usr/bin/env bash
<<COMM
# EXAMPLE VALUES
snappy_home="/home/hemanth/workspace/snappydata/build-artifacts/scala-2.11"
input_path="/home/hemanth/workspace/test/data/op"
table_name="APP.dummy"
num_of_secs_to_run=45
lead="localhost:8090"
app_jar="/home/hemanth/workspace/test/target/test-1.0.jar"
copy_freq=10
props_path="/home/hemanth/app.properties"
COMM

props_path=$1
. $props_path

its=`date '+%H_%M_%S'`
input_path_ts="${input_path}_$its"


if [ -d $input_path_ts ]
then
  echo "INFO: $input_path_ts already exists. deleting it"
  rm -r $input_path_ts
fi

#--------------------------------
#       export from table
#--------------------------------
submit_res=`$snappy_home/snappy/bin/snappy-job.sh submit  --lead $lead --app-name "${table_name}_export_csv" --class io.snappydata.hydra.concurrency.ExportTable --app-jar $app_jar --conf output_path=$input_path_ts --conf input_table=$table_name --conf limit=$streaming_sample_size`
job_id=`echo $submit_res | grep -o 'jobId":\s"[a-zA-Z0-9-]*' | sed 's/jobId":\s"//g'`
context=`echo $submit_res | grep -o 'context": "[a-zA-Z0-9]*' | sed 's/context": "//g'`

status="RUNNING"

while [ "$status" == "RUNNING" ]
do
  job_status=`$snappy_home/snappy/bin/snappy-job.sh status  --lead $lead --job-id $job_id`
  status=`echo $job_status | grep -o 'status": "[A-Z]*' | sed 's/status": "//g'`
  sleep 2
done

if [ "$status" == "FINISHED" ]
then
  echo "${table_name}_export_csv completed"
else
  echo "ERROR:${table_name}_export_csv failed with status=$status $job_status"
  exit 1
fi
echo "export completed. check $input_path_ts"

#--------------------------------
#       Streaming activity
#--------------------------------
submit_res=`$snappy_home/snappy/bin/snappy-job.sh submit  --lead $lead --app-name "${table_name}_stream_activity" --class io.snappydata.hydra.concurrency.StreamingActivity  --app-jar $app_jar  --conf prop_path=$props_path --conf input_path=$input_path_ts --batch-interval $batch_interval --stream`
sleep 5

echo $submit_res
job_id=`echo $submit_res | grep -o 'jobId":\s"[a-zA-Z0-9-]*' | sed 's/jobId":\s"//g'`
context=`echo $submit_res | grep -o 'context": "[a-zA-Z0-9]*' | sed 's/context": "//g'`

job_status=`$snappy_home/snappy/bin/snappy-job.sh status  --lead $lead --job-id $job_id`
status=`echo $job_status | grep -o 'status": "[A-Z]*' | sed 's/status": "//g'`

if [ "$status" == "RUNNING" ]
then
  echo "SUCCESS: jobId:$job_id started and running"
else
  echo "FAILURE: job not in running state. $job_status"
  exit 1891
fi

curr_ts=`date '+%T'`
stop_time=`date '+%T' --date="$curr_now + $num_of_secs_to_run seconds"`
echo "current time is:$curr_ts, APP will run till: $stop_time"

while [ "$status" == "RUNNING" ] && [ `date '+%T'` \< "$stop_time" ]
do
  job_status=`$snappy_home/snappy/bin/snappy-job.sh status  --lead $lead --job-id $job_id`
  status=`echo $job_status | grep -o 'status": "[A-Z]*' | sed 's/status": "//g'`
  echo "SLEEPING for $copy_freq secs and status is $status"
  sleep $copy_freq
  dt=`date "+%H_%M_%S"`
  echo "COPYING NEW FILE TO $input_path_ts/data_$dt"
  cp $input_path_ts/part-00000-* $input_path_ts/data_$dt
done

$snappy_home/snappy/bin/snappy-job.sh   stopcontext $context  --lead "$lead"

if [ -d $input_path_ts ]
then
  echo "cleaning up $input_path_ts"
  rm -r $input_path_ts
fi
