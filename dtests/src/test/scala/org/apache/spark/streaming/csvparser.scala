package org.apache.spark.streaming
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._ //for UserDefinedAggr
import org.apache.spark.sql.streaming.SchemaDStream
import org.apache.spark.sql.functions.{collect_list, udf, col}
import org.apache.spark.streaming.{SnappyStreamingContext, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast // for PATTERN_MAP, SENSOR_MAP
import scala.io.Source
import scala.util.parsing.json.JSON
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.text.SimpleDateFormat;

class CSVParser(sc:SparkContext)
                        extends java.io.Serializable {
  
    /** Define Schema of Dataframe */
    private val SRC1Schema = new StructType()
        .add(Col.ID     , "string")
        .add(Col.ACC_TS     , "string") //changed to TimeStamp type 
        .add(Col.ACC_DATA1     , "double")
        .add(Col.ACC_DATA2     , "double")
        .add(Col.ACC_DATA3     , "double")


    /** When Input format is horizon */
/*    private val SRC2Schema = new StructType()
        .add(Col.ID     , "string")
        .add(Col.BIO_TS     , "string") //changed to TimeStamp type
        .add("HEART_RATE"     , "double")
        .add("BREATH_RATE"     , "double")

    private val SRC3Schema = new StructType()
        .add(Col.ID     , "string")
        .add(Col.ENV_TS     , "string") //changed to TimeStamp type
        .add("TEMPERATURE"     , "double")
        .add("HUMIDITY"     , "double")
        .add("ILLUMINANCE"     , "double")*/

    
    /** When Input format is vartical */
    private val SRC2Schema = new StructType()
        .add(Col.ID, "string")
        .add(Col.BIO_TS, "string")
        .add(Col.SENSOR_CODE, "double")
        .add(Col.SENSOR_VALUE, "double")

    private val SRC3Schema = new StructType()
        .add(Col.ID, "string")
        .add(Col.ENV_TS, "string")
        .add(Col.SENSOR_CODE, "double")
        .add(Col.SENSOR_VALUE, "double")


    def toInterval(time: String): String = {
    
        val dateFormat = new SimpleDateFormat(Consts.TIME_FORMAT)
        val longts: Long = (dateFormat.parse(time)).getTime
        val longinterval:Long = longts - (longts % (1000 * Consts.GROUP_WIDTH.toLong))
        val intervalts =  new Timestamp(longinterval)
    
        return intervalts.toString.take(19)

    }


    /** Convert csv raw data to SchemaDStream */
    def parseCSV_SRC1(csv: DStream[String]
            , snsc2:SnappyStreamingContext): SchemaDStream = {
    
        val intervaledInput = csv
            .filter(r => r.take(7) != Col.ID) // remove Header
            .map{r => 
                  val s    = r.split("\t")
                  val time = s(1).take(19) // e.g. 2016-01-01 12:34:11
                  val interval = toInterval(time) // e.g. 2016-01-01 12:34:00
                  Row(s(0), interval, s(3).toDouble, s(4).toDouble, s(5).toDouble)
                }
   
        return snsc2.createSchemaDStream(intervaledInput, SRC1Schema)
    }


    def parseCSV_SRC2(csv: DStream[String]
            , snsc2:SnappyStreamingContext): SchemaDStream = {

        val intervaledInput = csv
            .filter(r => r.take(7) != Col.ID) // remove Header
            .map{r =>
                  val s    = r.split(",")
                  val time = s(1).take(19) // e.g. 2016-01-01 12:34:11
                  val interval = toInterval(time) // e.g. 2016-01-01 12:34:00
//                  Row(s(0), interval, s(2).toDouble, s(3).toDouble)
                  Row(s(0), interval, s(2).toDouble, s(4).toDouble)
                }

        return snsc2.createSchemaDStream(intervaledInput, SRC2Schema)
   }


    def parseCSV_SRC3(csv: DStream[String]
            , snsc2:SnappyStreamingContext): SchemaDStream = {

        val intervaledInput = csv
            .filter(r => r.take(7) != Col.ID) // remove Header
            .map{r =>
                  val s    = r.split(",")
                  val time = s(1).take(19) // e.g. 2016-01-01 12:34:11
                  val interval = toInterval(time) // e.g. 2016-01-01 12:34:00
//                  Row(s(0), interval, s(2).toDouble, s(3).toDouble, s(4).toDouble)
                  Row(s(0), interval, s(2).toDouble, s(4).toDouble)
                }

        return snsc2.createSchemaDStream(intervaledInput, SRC3Schema)
    }

}
