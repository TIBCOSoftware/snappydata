#!/usr/bin/env bash
source PerfRun.conf

#stop cluster
ssh $aggregator memsql-ops cluster-stop

#copy the memql.cnf in corresponding master/load's memsql root directory
sudo ssh $aggregator cp /QASNAPPY/TPCH/Memsql/memsql.cnf /var/lib/memsql/master-3306/memsql.cnf

for element in "${leafs[@]}"; 
  do 
	sudo ssh $element cp /QASNAPPY/TPCH/Memsql/memsql.cnf /var/lib/memsql/leaf-3306/memsql.cnf 
  done

ssh $aggregator memsql-ops cluster-start
