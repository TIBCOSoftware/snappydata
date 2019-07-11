#!/usr/bin/env bash

#
# Copyright (c) 2018 SnappyData, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License. You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See accompanying
# LICENSE file.
#

# Check whether "-dir" argument has been provide or not while invoing snappy-locator.sh/snappy-server.sh/snappy-lead.sh
#

noOfInputsArgs=$#

if [ $noOfInputsArgs -eq 0 ];then  #if no arguments passed
  echo "ERROR: No arguments have been provided. Please provide required arguments"
  exit 1
else

  # there could be two scenario if arguments are not equal to zero
    # - script get triggerd from snappy-start-all.sh--then argument provided by this snappy-nodes.sh
    # - user executing individual component script,in this case this script: snappy-locator.sh
  # Need to check in both case -dir option is provided or not
  isPresent=0      
  for argument in "$@"; do	
    if [[ "$argument" == -dir=*  ]]; then
      isPresent=1 #  present
      if [ -z $(echo $argument | cut -d'=' -f 2) ]; then #present but empty i.e "-dir="
        isPresent=0
      #else #present but should be a directory but do not need to check here, as getting check in launcher			
      fi			
    fi
  done
	
  if [ $isPresent -eq 0 ]; then
    echo "ERROR: Please provide -dir argument"
    exit 1
  fi
  exit 0  
fi
