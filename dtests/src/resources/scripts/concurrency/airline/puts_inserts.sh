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

lead=$2
app_jar=$3
copy_freq=$4
input_path=$5
table_name=$6
enable_putinto=$7
streaming_sample_size=$8
batch_interval=$9
snappy_home=${10}
num_of_secs_to_run=${11}

echo "table_name=$table_name" >> $props_path
echo "enable_putinto=$enable_putinto" >> $props_path
echo "snappy_home=$snappy_home" >> $props_path

its=`date '+%H_%M_%S'`
input_path_ts="${input_path}_$its"

if [ -d $input_path_ts ]
then
  rm -r $input_path_ts
fi

#--------------------------------
#       export from table
#--------------------------------
submit_res=`$snappy_home/snappy/bin/snappy-job.sh submit  --lead $lead --app-name "${table_name}_export_csv" --class io.snappydata.hydra.concurrency.ExportTable --app-jar $app_jar --conf output_path=$input_path_ts --conf input_table=$table_name --conf limit=$streaming_sample_size`
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
  echo "$job_status"
  sleep $copy_freq
  dt=`date "+%H_%M_%S"`
  echo "COPYING NEW FILT TO $input_path_ts/part_$dt"
  cp $input_path_ts/part-00000-* $input_path_ts/data_$dt
done

echo "sleeping for 200 secs before stopping context: $context"
sleep 200
$snappy_home/snappy/bin/snappy-job.sh   stopcontext $context  --lead "$lead"
