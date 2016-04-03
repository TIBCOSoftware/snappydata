--- Suppose a particular Airline company say 'Delta Air Lines Inc.' re-brands itself as 'Delta America'
--- the airline code can be updated in the row table
UPDATE AIRLINEREF SET DESCRIPTION='Delta America' WHERE CODE='DL';

--- Query to see the updated result ---
SELECT * FROM AIRLINEREF WHERE CAST(CODE AS VARCHAR(25))='DL';

