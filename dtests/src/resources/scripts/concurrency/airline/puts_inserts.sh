
<<COMM
# example values
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

#--------------------------------
#       export from table
#--------------------------------
submit_res=`$snappy_home/snappy/bin/snappy-job.sh submit  --lead $lead --app-name "${table_name}_export_csv" --class io.snappydata.hydra.concurrency.ExportTable --app-jar $app_jar --conf output_path=$input_path --conf input_table=$table_name --conf limit=1000`

job_id=`echo $submit_res | grep -o 'jobId":\s"[a-zA-Z0-9-]*' | sed 's/jobId":\s"//g'`
context=`echo $submit_res | grep -o 'context": "[a-zA-Z0-9]*' | sed 's/context": "//g'`

echo "submitted the job ${table_name}_export_csv. waiting for 60 seconds"
sleep 60
job_status=`$snappy_home/snappy/bin/snappy-job.sh status  --lead $lead --job-id $job_id`
status=`echo $job_status | grep -o 'status": "[A-Z]*' | sed 's/status": "//g'`

if [ "$status" == "FINISHED" ]
then
  echo "${table_name}_export_csv completed"
else
  echo "ERROR:${table_name}_export_csv failed with status=$status $job_status"
  exit 1
fi

#--------------------------------
#       Streaming activity
#--------------------------------
submit_res=`$snappy_home/snappy/bin/snappy-job.sh submit  --lead $lead --app-name "${table_name}_stream_activity" --class io.snappydata.hydra.concurrency.StreamingActivity  --app-jar $app_jar  --conf prop_path=$props_path --stream`
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
  echo "$job_status"
  sleep $copy_freq
  dt=`date "+%H_%M_%S"`
  echo "COPYING NEW FILT TO $input_path/part_$dt"
  cp $input_path/part-00000-* $input_path/data_$dt
done

sleep 5
$snappy_home/snappy/bin/snappy-job.sh   stopcontext $context  --lead "$lead"
