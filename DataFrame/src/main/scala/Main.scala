package questions

import org.apache.spark.sql.SparkSession

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._

object Main {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("main")
      .config("spark.driver.memory", "5g")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .getOrCreate()

      val filePath = getClass().getResource("/2008.csv").toString
      val airportsPath = getClass().getResource("/airports.csv").toStringval carriersPath = getClass().getResource("/carriers.csv").toString


    val processor = new AirTrafficProcessor(spark, filePath, airportsPath, carriersPath)
    val data = processor.loadDataAndRegister(filePath)

    println(data.schema)
    data.collect().foreach(println)
    println("<<<security>>>")
    // processor.cancelledDueToSecurity().show()
    // println("<<<weather dealy>>>")
    // processor.longestWeatherDelay().show()
    // println("<<<didn't fly>>>")
    //processor.didNotFly().show()
    // println("<<<from vegas to jfk>>>")
    // processor.flightsFromVegasToJFK().show()
    // println("<<<time taxiing>>>")
    // processor.timeSpentTaxiing().show()
    // println("<<<median>>>")
    // processor.distanceMedian().show()
    // println("<<<percentile>>>")
    // processor.score95().show()
    // println("<<<cancelled flights>>>")
    // processor.cancelledFlights().show()
    //println("least squares: " + processor.leastSquares())
    

    spark.stop()
  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import spark.implicits._

val spark = SparkSession.builder.master("local[*]").appName("main").config("spark.driver.memory", "5g").config("spark.dynamicAllocation.enabled","true").config("spark.shuffle.service.enabled","true").getOrCreate()
val filePath = "src/main/resources/2008.csv"
val airportsPath = "src/main/resources/airports.csv"
val carriersPath = "src/main/resources/carriers.csv"

val airTraffic = "airtraffic"
val carriers = "carriers"
val airports = "airports"

val carriersTable = spark.read.option("header","true").option("inferSchema", "true").csv(carriersPath)
carriersTable.createOrReplaceTempView("carriers")

val airportsTable = spark.read.option("header","true").option("inferSchema", "true").csv(airportsPath)
airportsTable.createOrReplaceTempView("airports")


def loadDataAndRegister(path: String): DataFrame = {

  import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

  val customSchema = StructType(Array(
  StructField("Year", IntegerType,true),
  StructField("Month", IntegerType,true),
  StructField("DayofMonth", IntegerType,true),
  StructField("DayOfWeek", IntegerType,true),
  StructField("DepTime", IntegerType,true),
  StructField("CRSDepTime", IntegerType,true),
  StructField("ArrTime", IntegerType,true),
  StructField("CRSArrTime", IntegerType,true),
  StructField("UniqueCarrier", StringType,true),
  StructField("FlightNum", IntegerType,true),
  StructField("TailNum", StringType,true),
  StructField("ActualElapsedTime", IntegerType,true),
  StructField("CRSElapsedTime", IntegerType,true),
  StructField("AirTime", IntegerType,true),
  StructField("ArrDelay", IntegerType,true),
  StructField("DepDelay", IntegerType,true),
  StructField("Origin", StringType,true),
  StructField("Dest", StringType,true),
  StructField("Distance", IntegerType,true),
  StructField("TaxiIn", IntegerType,true),
  StructField("TaxiOut", IntegerType,true),
  StructField("Cancelled", IntegerType,true),
  StructField("CancellationCode", StringType,true),
  StructField("Diverted", IntegerType,true),
  StructField("CarrierDelay", IntegerType,true),
  StructField("WeatherDelay", IntegerType,true),
  StructField("NASDelay", IntegerType,true),
  StructField("SecurityDelay", IntegerType,true),
  StructField("LateAircraftDelay", IntegerType,true)))

  val data = spark.read
  .option("header","true")
  .option("nullValue", "NA")
  .schema(customSchema)
  .csv(path)

  data.createOrReplaceTempView(airTraffic)
  return data
}

val data = loadDataAndRegister(filePath)

spark.catalog.listTables().show()














