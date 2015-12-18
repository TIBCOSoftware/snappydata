----Suppose a particular Airline company say 'Jet Airways Limited' having CODE '9W' acquires 'Titan Airways' having CODE '02Q' ,then we need to update the snappy row table as below:----
UPDATE AIRLINEREF SET DESCRIPTION='Jet Airways Limited' WHERE CAST(CODE AS VARCHAR(25))='02Q';
