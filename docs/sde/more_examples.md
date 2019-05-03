# More Examples

## Example 1
Create a sample table with qcs 'medallion' 
```pre
CREATE SAMPLE TABLE NYCTAXI_SAMPLEMEDALLION ON NYCTAXI 
  OPTIONS (buckets '8', qcs 'medallion', fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM NYCTAXI);
```

**SQL Query:**
```pre
select medallion,avg(trip_distance) as avgTripDist,
  absolute_error(avgTripDist),relative_error(avgTripDist),
  lower_bound(avgTripDist),upper_bound(avgTripDist) 
  from nyctaxi group by medallion order by medallion desc limit 100
  with error;
  //These built-in error functions is explained in a section below.
```

**DataFrame API Query:**
```pre
snc.table(basetable).groupBy("medallion").agg( avg("trip_distance").alias("avgTripDist"),
  absolute_error("avgTripDist"),  relative_error("avgTripDist"), lower_bound("avgTripDist"),
  upper_bound("avgTripDist")).withError(.6, .90, "do_nothing").sort(col("medallion").desc).limit(100)
```

## Example 2
Create additional sample table with qcs 'hack_license' 

```pre
CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE ON NYCTAXI OPTIONS
(buckets '8', qcs 'hack_license', fraction '0.01', strataReservoirSize '50') AS (SELECT * FROM NYCTAXI);
```

**SQL Query:**
```pre
select  hack_license, count(*) count from NYCTAXI group by hack_license order by count desc limit 10 with error
// the engine will automitically use the HackLicense sample for a more accurate answer to this query.
```

**DataFrame API Query:**
```pre
snc.table(basetable).groupBy("hack_license").count().withError(.6,.90,"do_nothing").sort(col("count").desc).limit(10)
```

## Example 3
Create a sample table using function "hour(pickup_datetime) as QCS

```pre
Sample Tablecreate sample table nyctaxi_hourly_sample on nyctaxi options (buckets '8', qcs 'hourOfDay', fraction '0.01', strataReservoirSize '50') AS (select *, hour(pickupdatetime) as hourOfDay from nyctaxi);
```

**SQL Query:**
```pre
select sum(trip_time_in_secs)/60 totalTimeDrivingInHour, hour(pickup_datetime) from nyctaxi group by hour(pickup_datetime)
```

**DataFrame API Query:**
```pre
snc.table(basetable).groupBy(hour(col("pickup_datetime"))).agg(Map("trip_time_in_secs" -> "sum")).withError(0.6,0.90,"do_nothing").limit(10)
```

## Example 4
If you want a higher assurance of accurate answers for your query, match the QCS to "group by columns" followed by any filter condition columns. Here is a sample using multiple columns.

```pre
Sample Tablecreate sample table nyctaxi_hourly_sample on nyctaxi options (buckets '8', qcs 'hack_license, year(pickup_datetime), month(pickup_datetime)', fraction '0.01', strataReservoirSize '50') AS (select *, hour(pickupdatetime) as hourOfDay from nyctaxi);
```

**SQL Query:**
```pre
Select hack_license, sum(trip_distance) as daily_trips from nyctaxi  where year(pickup_datetime) = 2013 and month(pickup_datetime) = 9 group by hack_license  order by daily_trips desc
```

**DataFrame API Query:**
```pre
snc.table(basetable).groupBy("hack_license","pickup_datetime").agg(Map("trip_distance" -> "sum")).alias("daily_trips").       filter(year(col("pickup_datetime")).equalTo(2013) and month(col("pickup_datetime")).equalTo(9)).withError(0.6,0.90,"do_nothing").sort(col("sum(trip_distance)").desc).limit(10)
```
