#!/bin/bash

# Starting new instance in VPC often results that `hostname` returns something like 'ip-10-1-1-24', which is
# not resolvable. Which leads to problems like SparkUI failing to bind itself on start up to that hostname as
# described in https://issues.apache.org/jira/browse/SPARK-5246.
# This script maps private ip to such hostname via '/etc/hosts'.
#

# # Are we in VPC?
# MAC=`wget -q -O - http://169.254.169.254/latest/meta-data/mac`
# VCP_ID=`wget -q -O - http://169.254.169.254/latest/meta-data/network/interfaces/macs/${MAC}/vpc-id`
# if [ -z "${VCP_ID}" ]; then
#     # echo "nothing to do - instance is not in VPC"
#     exit 0
# fi

PRIVATE_IP=`wget -q -O - http://169.254.169.254/latest/meta-data/local-ipv4`

PUBLIC_HOSTNAME=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`

if [[ ! -e /etc/hosts.orig ]]; then
  sudo cp /etc/hosts /etc/hosts.orig
fi

cp /etc/hosts.orig /home/ec2-user/hosts
echo -e "\n# fixed by resolve-hostname.sh \n${PRIVATE_IP} ${PUBLIC_HOSTNAME}\n" >> /home/ec2-user/hosts
sudo mv /home/ec2-user/hosts /etc/hosts
echo "Updated /etc/hosts of ${PUBLIC_HOSTNAME}"

if [[ -e ~/.ssh/known_hosts ]] && [[ ! -e ~/.ssh/known_hosts.orig ]]; then
  cp ~/.ssh/known_hosts ~/.ssh/known_hosts.orig
fi

# Let's make sure that it did not break anything
ping -c 1 -q "${PUBLIC_HOSTNAME}" > /dev/null 2>&1
if [ $? -ne 0 ]; then
  # return some non-zero code to indicate problem
  echo "Possible bug: unable to fix resolution of local hostname"
  return 62
fi

