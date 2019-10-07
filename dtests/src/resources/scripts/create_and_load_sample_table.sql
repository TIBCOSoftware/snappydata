--- DROP TABLE IF ALREADY EXISTS ---
DROP TABLE IF EXISTS AIRLINE_SAMPLE ;

--- CREATE SAMPLE TABLE ---
CREATE SAMPLE TABLE AIRLINE_SAMPLE ON AIRLINE
  OPTIONS(
    buckets '8',
    qcs 'UniqueCarrier, Year_, Month_',
    fraction '0.03',
    strataReservoirSize '50', PERSISTENT ':persistenceMode')
  AS (
    SELECT Year_, Month_ , DayOfMonth,
      DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
      UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
      CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
      Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
      Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
      LateAircraftDelay, ArrDelaySlot
    FROM AIRLINE);