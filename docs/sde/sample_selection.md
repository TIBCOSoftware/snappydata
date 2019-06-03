# Sample Selection

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent> 

Sample selection logic selects most appropriate sample, based on this relatively simple logic in the current version:

* If the query is not an aggregation query (based on COUNT, AVG, SUM) then reject the use of any samples. The query is executed on the base table. Else,

* If query QCS (columns involved in Where/GroupBy/Having matched the sample QCS, then, select that sample

* If exact match is not available, then, if the sample QCS is a superset of query QCS, that sample is used

* If superset of sample QCS is not available, a sample where the sample QCS is a subset of query QCS is used

* When multiple stratified samples with a subset of QCSs match, a sample with most matching columns is used. The largest size of the sample gets selected if multiple such samples are available. 
