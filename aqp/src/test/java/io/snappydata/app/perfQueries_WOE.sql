Select sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline  with error;
Select sum(ArrDelay) as x from sampleTable_WOE;
Select sum(ArrDelay) as x from airline;
Select  uniqueCarrier, sum(ArrDelay) as x , absolute_error(x),relative_error(x)  from airline group by uniqueCarrier with error;
Select  uniqueCarrier, sum(ArrDelay) as x from sampleTable_WOE group by uniqueCarrier;
Select  uniqueCarrier, sum(ArrDelay) as x from airline group by uniqueCarrier;