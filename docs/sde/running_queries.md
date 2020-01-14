# Running Queries

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent> 

Queries can be executed directly on sample tables or on the base table. Any query executed on the sample directly will always result in an approximate answer. When queries are executed on the base table users can specify their error tolerance and additional behavior to permit approximate answers. The Engine will automatically figure out if the query can be executed by any of the available samples. If not, the query can be executed on the base table based on the behavior clause. 

Here is the syntax:

```pre
> > SELECT ... FROM .. WHERE .. GROUP BY ...<br>
> > WITH ERROR `<fraction> `[CONFIDENCE` <fraction>`] [BEHAVIOR `<string>]`
```
    
* **WITH ERROR** - this is a mandatory clause. The values are  0 < value(double) < 1 . 
* **CONFIDENCE** - this is optional clause. The values are confidence 0 < value(double) < 1 . The default value is 0.95
* **BEHAVIOR** - this is an optional clause. The values are `do_nothing`, `local_omit`, `strict`,  `run_on_full_table`, `partial_run_on_base_table`. The default value is `run_on_full_table`	

These 'behavior' options are fully described in the section below. 

Here are some examples:

```pre
SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc 
  with error 0.10 
// tolerate a maximum error of 10% in each row in the answer with a default confidence level of 0.95.

SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc 
  with error 
// tolerate any error in the answer. Just give me a quick response.

SELECT sum(ArrDelay) ArrivalDelay, Month_ from airline group by Month_ order by Month_ desc with error 0.10 confidence 0.95 behavior ‘local_omit’
// tolerate a maximum error of 10% in each row in the answer with a confidence interval of 0.95.
// If the error for any row is greater than 10% omit the answer. i.e. the row is omitted. 
```

## Using the Spark DataFrame API

The Spark DataFrame API is extended with support for approximate queries. Here is 'withError' API on DataFrames.
```pre
def withError(error: Double,
confidence: Double = Constant.DEFAULT_CONFIDENCE,
behavior: String = "DO_NOTHING"): DataFrame
```

Query examples using the DataFrame API
```pre
snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).orderBy( desc("Month_")).withError(0.10) 
snc.table(baseTable).agg(Map("ArrDelay" -> "sum")).orderBy( desc("Month_")).withError(0.10, 0.95, 'local_omit’) 
```

## Supporting BI Tools or Existing Apps
To allow BI tools and existing Apps that say might be generating SQL, SDE also supports specifying these options through your SQL connection or using the Snappy SQLContext. 

```pre
snContext.sql(s"spark.sql.aqp.error=$error")
snContext.sql(s"spark.sql.aqp.confidence=$confidence")
snContext.sql(s"set spark.sql.aqp.behavior=$behavior")
```
These settings will apply to all queries executed via this SQLContext. Application can override this by also using the SQL extensions specified above.

Applications or tools using JDBC/ODBC can set the following properties:
 
*	When using Apache Zeppelin JDBC interpreter or the Snappy SQL you can set the values as follows:

    ```pre
    set spark.sql.aqp.error=$error;
    set spark.sql.aqp.confidence=$confidence;
    set spark.sql.aqp.behavior=$behavior;
    ```
*	Setting AQP specific properties as a connection level property using JDBC:

          Properties prop = new Properties();
          prop.setProperty("spark.sql.aqp.error","0.3");
          prop.setProperty("spark.sql.aqp.behavior","local_omit");
