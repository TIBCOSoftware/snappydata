#!/bin/bash
echo "<<<< Tibco ComputeDB >>>>"
Protocol=http
JenkinsServerIP=34.208.45.182
JenkinsPort=8080

oss=o
enterprise=e
correctBuildTypeMentioned=true

read -p "Enter build type OSS (o) or Enterprise (e) ?  : " version

while [ $correctBuildTypeMentioned ]
do
  if [ "$version" = "$oss" ] || [ "$version" = "$enterprise" ] ; then
    break
 else
    read -p "Enter valid build type OSS (o) or Enterprise (e) ?  : " version
  fi
done

echo ""
read -p "Jenkins user name : " jenkinsUserName
echo ""

stty -echo
printf "Jenkins user password : "
read jenkinsUserPassword
stty echo
echo ""
echo ""

read -p "Snapydata repo branch (master - jenkins-ci): " snappydataBranch
echo ""

read -p "Spark repo branch (master - snappy/branch-2.1): " sparkBranch
echo ""

read -p "Store repo branch (master - snappy/master): " storeBranch
echo ""

read -p "AQP repo branch (master - */master): " aqpBranch
echo ""

read -p "JobServer repo branch (master - snappydata): " JobServerBranch
echo ""

read -p "precheckin target (e.g precheckin -Pstore -PSpark): " target
echo ""

# Sending a crumb request
curl -k -s -u $jenkinsUserName:$jenkinsUserPassword "$Protocol://$JenkinsServerIP:$JenkinsPort"/crumbIssuer/api/json > aa.out
crumb="`grep -Po '"crumb": *\K"[^"]*"' aa.out | sed -e 's/^"//' -e 's/"$//'`"
rm -f aa.out

if [ $version = "o" ]
then
  curl -X POST -G "$Protocol://$JenkinsServerIP:$JenkinsPort/job/tibco-computedb-ci/buildWithParameters"  \
  --data-urlencode "username=$jenkinsUserName" \
  --data-urlencode "snappybranch=$snappydataBranch" \
  --data-urlencode "sparkbranch=$sparkBranch" \
  --data-urlencode "snappystorebranch=$storeBranch" \
  --data-urlencode "sparkjobserverbranch=$JobServerBranch" \
  --data-urlencode "aqpbranch=$aqpBranch" \
  --data-urlencode "target=$target" \
  --user $jenkinsUserName:$jenkinsUserPassword \
  -H 'Jenkins-Crumb':$crumb
  echo "Tibco ComputeDB OSS Job submitted to the Jenkins server.  For more information you can check @ $Protocol://$JenkinsServerIP:$JenkinsPort/job"
  echo ""
elif [ $version = "e" ]
then
  curl -X POST -G "$Protocol://$JenkinsServerIP:$JenkinsPort/job/tibco-computedb-ci-enterprise/buildWithParameters" \
  --data-urlencode "username=$jenkinsUserName" \
  --data-urlencode "snappybranch=$snappydataBranch" \
  --data-urlencode "sparkbranch=$sparkBranch" \
  --data-urlencode "snappystorebranch=$storeBranch" \
  --data-urlencode "sparkjobserverbranch=$JobServerBranch" \
  --data-urlencode "aqpbranch=$aqpBranch" \
  --data-urlencode "target=$target" \
  --user $jenkinsUserName:$jenkinsUserPassword \
  -H 'Jenkins-Crumb':$crumb
  echo "Tibco ComputeDB Enterprise Job submitted to the Jenkins server. For more information you can check @ $Protocol://$JenkinsServerIP:$JenkinsPort/job"
  echo ""
else
  echo "Failed to submit Jenkins job as build type not provided correctly."
fi
