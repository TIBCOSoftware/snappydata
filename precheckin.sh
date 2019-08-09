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

read -p "Enter your email Id for build status notification : " useremail
echo ""

read -p "Snapydata repo branch (Default-branch - jenkins-ci): " snappydataBranch
if [ -z "$snappydataBranch" ]
then
      snappydataBranch=jenkins-ci
fi
echo ""

read -p "Spark repo branch (Default-branch - snappy/branch-2.1): " sparkBranch
if [ -z "$sparkBranch" ]
then
      sparkBranch=snappy/branch-2.1
fi
echo ""

read -p "Store repo branch (Default-branch - snappy/master): " storeBranch
if [ -z "$storeBranch" ]
then
      storeBranch=snappy/master
fi
echo ""

#Skip the AQP option if its OSS build.
if [ "$version" = "$enterprise" ]
then
  read -p "AQP repo branch (Default-branch - */master): " aqpBranch
  if [ -z "$aqpBranch" ]
  then
        aqpBranch=*/master
  fi
  echo ""
  read -p "Snappy-connectors repo branch (Default-branch - master): " snappyconnectorsbranch
  if [ -z "$snappyconnectorsbranch" ]
  then
        snappyconnectorsbranch=master
  fi
  echo ""
fi
echo ""

read -p "JobServer repo branch (Default-branch - snappydata): " JobServerBranch
if [ -z "$JobServerBranch" ]
then
      JobServerBranch=snappydata
fi
echo ""


read -p "gradle precheckin target. (e.g precheckin -Pspark -Pstore): " target
if [ -z "$target" ]
then
      target=precheckin
fi
echo ""


# Sending a crumb request
curl -k -s -u $jenkinsUserName:$jenkinsUserPassword "$Protocol://$JenkinsServerIP:$JenkinsPort"/crumbIssuer/api/json > aa.out
crumb="`grep -Po '"crumb": *\K"[^"]*"' aa.out | sed -e 's/^"//' -e 's/"$//'`"
rm -f aa.out

if [ $version = "o" ]
then
  curl -X POST -G "$Protocol://$JenkinsServerIP:$JenkinsPort/job/tibco-computedb-ci/buildWithParameters"  \
  --data-urlencode "username=$jenkinsUserName" \
  --data-urlencode "useremail=$useremail" \
  --user $jenkinsUserName:$jenkinsUserPassword \
  --data-urlencode "snappybranch=$snappydataBranch" \
  --data-urlencode "sparkbranch=$sparkBranch" \
  --data-urlencode "snappystorebranch=$storeBranch" \
  --data-urlencode "sparkjobserverbranch=$JobServerBranch" \
  --data-urlencode "aqpbranch=$aqpBranch" \
  --data-urlencode "target=$target" \
  -H 'Jenkins-Crumb':$crumb
  echo "Tibco ComputeDB OSS Job submitted to the Jenkins server.  For more information you can check @ $Protocol://$JenkinsServerIP:$JenkinsPort/job"
  echo ""
elif [ $version = "e" ]
then
  curl -X POST -G "$Protocol://$JenkinsServerIP:$JenkinsPort/job/tibco-computedb-ci-enterprise/buildWithParameters" \
  --data-urlencode "username=$jenkinsUserName" \
  --data-urlencode "useremail=$useremail" \
  --user $jenkinsUserName:$jenkinsUserPassword \
  --data-urlencode "snappybranch=$snappydataBranch" \
  --data-urlencode "sparkbranch=$sparkBranch" \
  --data-urlencode "snappystorebranch=$storeBranch" \
  --data-urlencode "sparkjobserverbranch=$JobServerBranch" \
  --data-urlencode "aqpbranch=$aqpBranch" \
  --data-urlencode "snappyconnectorsbranch=$snappyconnectorsbranch" \
  --data-urlencode "target=$target" \
  -H 'Jenkins-Crumb':$crumb
  echo "Tibco ComputeDB Enterprise Job submitted to the Jenkins server. For more information you can check @ $Protocol://$JenkinsServerIP:$JenkinsPort/job"
  echo ""
else
  echo "Failed to submit Jenkins job as build type not provided correctly."
fi
