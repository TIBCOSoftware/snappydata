# SDE Properties

The properties

| Properties | Description |Command|
|--------|--------|--------|
|FlushReservoirThreshold|Reservoirs of sample table will be flushed and stored in columnar format if sampling is done on baset table of size more than flushReservoirThreshold. Default value is 10,000.|snappy.flushReservoirThreshold|
|NumBootStrapTrials|Number of bootstrap trials to do for calculating error bounds. Default value is 100.|spark.sql.aqp.numBootStrapTrials|
|Error|Maximum relative error tolerable in the approximate value calculation. It should be a fractional value not exceeding 1. Default value is 0.2}|spark.sql.aqp.error|
|Confidence|Confidence with which the error bounds are calculated for the approximate value. It should be a fractional value not exceeding 1. Default value is 0.95|spark.sql.aqp.confidence|
|Behavior|The action to be taken if the error computed goes oustide the error tolerance limit. Default value is DO_NOTHING|sparksql.aqp.behavior|