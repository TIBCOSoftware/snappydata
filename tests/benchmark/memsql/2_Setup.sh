#!/usr/bin/env bash
source PerfRun.conf

#start memsql-ops on aggregator
ssh $aggregator sudo memsql-ops start

ssh $aggregator sudo memsql-ops agent-start --all

ssh $aggregator  memsql-ops memsql-deploy -r master -P 3306 --developer-edition

for element in "${leafs[@]}";
  do
        sudo ssh $element memsql-ops memsql-deploy -r leaf -P 3306 --developer-edition
  done

##Now go to webbrowser to setup a cluster

#map localhost port to rdu-w27's 9000 port
#ssh -L 9999:localhost:9999 
#ssh -L 9999:localhost:9000 
