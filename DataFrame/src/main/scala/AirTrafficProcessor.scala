package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
/** airtrafficProcessor provides functionalites to
* process air traffic data
* Spark SQL tables available from start:
*   - 'carriers' airliner information
*   - 'airports' airport information
*
* After implementing the first method 'loadData'
* table 'airtraffic',which provides flight information,
* is also available. You can find the raw data from /resources folder.
* Useful links:
* http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
* http://spark.apache.org/docs/latest/sql-programming-guide.html
* https://github.com/databricks/spark-csv
*
* We are using data from http://stat-computing.org/dataexpo/2009/the-data.html
*
* @param spark reference to SparkSession
* @param filePath path to the air traffic data
* @param airportsPath path to the airport data
* @param carriersPath path to the carrier data
*/
class AirTrafficProcessor(spark: SparkSession,
    filePath:String, airportsPath: String, carriersPath: String) {

    import spark.implicits._

    /*
    * Table names for SQL
    * use airtraffic for table name in loadData
    * method.
    */
    val airtraffic = "airtraffic"
    val carriers = "carriers"
    val airports = "airports"

    // load the files and create tables
    // for spark sql
    //DO NOT EDIT
    val carriersTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(carriersPath)
    carriersTable.createOrReplaceTempView(carriers)
    //DO NOT EDIT
    val airportsTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(airportsPath)
    airportsTable.createOrReplaceTempView(airports)

    /** load the data and register it as a table
    * so that we can use it later for spark SQL.
    * File is in csv format, so we are using
    * Spark csv library.
    *
    * Use the variable airtraffic for table name.
    *
    * Example on how the csv library should be used can be found:
    * https://github.com/databricks/spark-csv
    *
    * Note:
    *   If you just load the data using 'inferSchema' -> 'true'
    *   some of the fields which should be Integers are casted to
    *   Strings. That happens, because NULL values are represented
    *   as 'NA' strings in this data.
    *
    *   E.g:
    *   2008,7,2,3,733,735,858,852,DL,1551,N957DL,85,77,42,6,-2,CAE,
    *   ATL,191,15,28,0,,0,NA,NA,NA,NA,NA
    *
    *   Therefore you should remove 'NA' strings and replace them with
    *   NULL. Option 'nullValue' in csv library is useful. However you
    *   don't need to take empty Strings into account.
    *   For instance TailNum field contains empty strings.
    *
    * Correct Schema:
    *   |-- Year: integer (nullable = true)
    *   |-- Month: integer (nullable = true)
    *   |-- DayofMonth: integer (nullable = true)
    *   |-- DayOfWeek: integer (nullable = true)
    *   |-- DepTime: integer (nullable = true)
    *   |-- CRSDepTime: integer (nullable = true)
    *   |-- ArrTime: integer (nullable = true)
    *   |-- CRSArrTime: integer (nullable = true)
    *   |-- UniqueCarrier: string (nullable = true)
    *   |-- FlightNum: integer (nullable = true)
    *   |-- TailNum: string (nullable = true)
    *   |-- ActualElapsedTime: integer (nullable = true)
    *   |-- CRSElapsedTime: integer (nullable = true)
    *   |-- AirTime: integer (nullable = true)
    *   |-- ArrDelay: integer (nullable = true)
    *   |-- DepDelay: integer (nullable = true)
    *   |-- Origin: string (nullable = true)
    *   |-- Dest: string (nullable = true)
    *   |-- Distance: integer (nullable = true)
    *   |-- TaxiIn: integer (nullable = true)
    *   |-- TaxiOut: integer (nullable = true)
    *   |-- Cancelled: integer (nullable = true)
    *   |-- CancellationCode: string (nullable = true)
    *   |-- Diverted: integer (nullable = true)
    *   |-- CarrierDelay: integer (nullable = true)
    *   |-- WeatherDelay: integer (nullable = true)
    *   |-- NASDelay: integer (nullable = true)
    *   |-- SecurityDelay: integer (nullable = true)
    *   |-- LateAircraftDelay: integer (nullable = true)
    *
    * @param path absolute path to the csv file.
    * @return created DataFrame with correct column types.
    */
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
          .option("nullValue","NA")
          .schema(customSchema)
          .csv(path)

        data.createOrReplaceTempView(airtraffic)

        return data
    }

    //USE can use SPARK SQL or DataFrame transformations

    /** Gets the number of flights for each
    * airplane. 'TailNum' column is unique for each
    * airplane so it should be used. DataFrame should
    * also be sorted by count in descending order.
    *
    * Result looks like:
    *   +-------+-----+
    *   |TailNum|count|
    *   +-------+-----+
    *   | N635SW| 2305|
    *   | N11150| 1342|
    *   | N572UA| 1176|
    *   | N121UA|    8|
    *   +-------+-----+
    *
    * @param df Air traffic data
    * @return DataFrame containing number of flights per
    * TailNum. DataFrame is sorted by count. Column names
    * are TailNum and count
    */
    def flightCount(df: DataFrame): DataFrame = {
        spark.sql("select TailNum, COUNT(TailNum) as count from airtraffic group by TailNum order by count desc")
    }


    /** Which flights were cancelled due to
    * security reasons the most?
    *
    * Example output:
    * +---------+----+
    * |FlightNum|Dest|
    * +---------+----+
    * |     4285| DHN|
    * |     4790| ATL|
    * |     3631| LEX|
    * |     3632| DFW|
    * +---------+----+
    *
    * @return Returns a DataFrame containing flights which were
    * cancelled due to security reasons (CancellationCode = 'D').
    * Columns FlightNum and Dest are included.
    */
    def cancelledDueToSecurity(df: DataFrame): DataFrame = {
        spark.sql("select FlightNum, Dest from airtraffic where CancellationCode = 'D'")
    }

    /** What was the longest weather delay between January
    * and march (1.1-31.3)?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |1148|
    * +----+
    *
    * @return DataFrame containing the highest delay which
    * was due to weather.
    */
    def longestWeatherDelay(df: DataFrame): DataFrame = {
        spark.sql("select max(WeatherDelay) from airtraffic where Month>=1 and Month<=3")
    }

    /** Which airliners didn't fly.
    * Table 'carriers' has this information.
    *
    * Example output:
    * +--------------------+
    * |         Description|
    * +--------------------+
    * |      Air Comet S.A.|
    * |   Aerodynamics Inc.|
    * |  Combs Airways Inc.|
    * |   Evanston Aviation|
    * |Lufthansa Cargo A...|
    * +--------------------+
    *
    * @return airliner descriptions
    */
    def didNotFly(df: DataFrame): DataFrame = {
        spark.sql("select Description from carriers where Code not in (select UniqueCarrier from airtraffic) sort by Description")
    }

    /** Find the airliners which travel
    * from Vegas to JFK. Sort them in descending
    * order by number of flights and append the
    * DataFrame with the data from carriers.csv.
    * Spark SQL table 'carriers' contains this data.
    * Vegas iasa code: LAS
    * JFK iasa code: JFK
    *
    *   Output should look like:
    *
    *   +--------------------+----+
    *   |         Description| Num|
    *   +--------------------+----+
    *   |     JetBlue Airways|1824|
    *   |Delta Air Lines Inc.|1343|
    *   |US Airways Inc. (...| 948|
    *   |American Airlines...| 366|
    *   +--------------------+----+
    *
    * @return DataFrame containing Columns Description
    * (airliner name) and Num(number of flights) sorted
    * in descending order.
    */
    def flightsFromVegasToJFK(df: DataFrame): DataFrame = {
        val codes = spark.sql("select UniqueCarrier, COUNT(UniqueCarrier) as Num from airtraffic where Origin='LAS' and Dest='JFK' group by UniqueCarrier order by Num desc")
        codes.createOrReplaceTempView("codes")
        spark.sql("select Description, Num from codes inner join carriers on codes.UniqueCarrier=carriers.Code")
    }


    /** How much time airplanes spend on moving from
    * gate to the runway and vise versa at the airport on average.
    * This method should return a DataFrame containing this
    * information per airport.
    * Columns 'TaxiIn' and 'TaxiOut' tells time spend on taxiing.
    * Order by time spend on taxiing in ascending order. 'TaxiIn'
    * means time spend on taxiing in departure('Origin') airport
    * and 'TaxiOut' spend on taxiing in arrival('Dest') airport.
    * Column name's should be 'airport' for ISA airport code and
    * 'taxi' for average time spend taxiing
    *
    *
    * DataFrame should look like:
    *   +-------+-----------------+
    *   |airport|             taxi|
    *   +-------+-----------------+
    *   |    BRW|5.084736087380412|
    *   |    OME|5.961294471783976|
    *   |    OTZ|6.866595496262391|
    *   |    DAL|6.983973195822733|
    *   |    HRL|7.019248180512919|
    *   |    SCC|7.054629009320311|
    *   +-------+-----------------+
    *
    * @return DataFrame containing time spend on taxiing per
    * airport ordered in ascending order.
    */
    def timeSpentTaxiing(df: DataFrame): DataFrame = {
        val in = spark.sql("select Origin, TaxiIn from airtraffic")
        in.createOrReplaceTempView("in")
        val out = spark.sql("select Dest, TaxiOut from airtraffic")
        out.createOrReplaceTempView("out")
        val all = spark.sql("select in.Origin as airport, in.TaxiIn as taxi from in union all select out.Dest, out.TaxiOut from out")
        all.createOrReplaceTempView("all")
        spark.sql("select airport, AVG(taxi) as taxi from all group by airport order by taxi")
    }

    /** What is the median travel distance?
    * Field Distance contains this information.
    *
    * Example output:
    * +-----+
    * |  _c0|
    * +-----+
    * |581.0|
    * +-----+
    *
    * @return DataFrame containing the median value
    */
    def distanceMedian(df: DataFrame): DataFrame = {
        spark.sql("SELECT percentile_approx(Distance, 0.5) FROM airtraffic")
    }

    /** What is the carrier delay, below which 95%
    * of the observations may be found?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |77.0|
    * +----+
    *
    * @return DataFrame containing the carrier delay
    */
    def score95(df: DataFrame): DataFrame = {
        spark.sql("SELECT percentile_approx(cast(CarrierDelay as float), 0.95,10000000) FROM airtraffic")
    }

  spark.sql(" select PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY CarrierDelay DESC) over CarrierDelay from airtraffic")

    /** From which airport are flights cancelled the most?
    * What percentage of flights are cancelled from a specific
    * airport?
    * cancelledFlights combines flight data with
    * location data. Returns a DataFrame containing
    * columns 'airport' and 'city' from 'airports'
    * and the sum of number of cancelled flights divided
    * by the number of all flights (we get percentage)
    * from 'airtraffic' table. Name should be 'percentage'.
    * Lastly result should be ordered by 'percentage' and
    * secondly by 'airport' both in descending order.
    *
    *
    *
    *
    *   Output should look something like this:
    *   +--------------------+--------------------+--------------------+
    *   |             airport|                city|          percentage|
    *   +--------------------+--------------------+--------------------+
    *   |  Telluride Regional|           Telluride|   0.211340206185567|
    *   |  Waterloo Municipal|            Waterloo| 0.17027863777089783|
    *   |Houghton County M...|             Hancock| 0.12264150943396226|
    *   |                Adak|                Adak| 0.09803921568627451|
    *   |Aspen-Pitkin Co/S...|               Aspen| 0.09157716223855286|
    *   |      Sioux Gateway |          Sioux City| 0.09016393442622951|
    *   +--------------------+--------------------+--------------------+
    *
    * @return DataFrame containing columns airport, city and percentage
    */
    def cancelledFlights(df: DataFrame): DataFrame = {
        val names = spark.sql("select iata, airport, city from airports")
        names.createOrReplaceTempView("names")
        val cancelled = spark.sql("select Origin, count(Origin) as count from airtraffic where Cancelled = 1 group by Origin")
        val all = spark.sql("select Origin, count(Origin) as count from airtraffic group by Origin")
        cancelled.createOrReplaceTempView("cancelled")
        all.createOrReplaceTempView("all")
        val perc = spark.sql("select all.Origin, cancelled.count/all.count as percentage from cancelled full join all on cancelled.Origin=all.Origin")
        perc.createOrReplaceTempView("perc")
        spark.sql("select names.airport, names.city, perc.percentage from names join perc on names.iata = perc.Origin order by perc.percentage desc, names.airport desc")
    }

    /**
    * Calculates the linear least squares approximation for relationship
    * between DepDelay and WeatherDelay.
    *  - First filter out entries where DepDelay < 0
    *  - there are definitely multiple data points for a single
    *    DepDelay value so calculate the average WeatherDelay per
    *    DepDelay
    *  - Calculate the linear least squares (c+bx=y) where
    *    x equals DepDelay and y WeatherDelay. c is the constant term
    *    and b is the slope.
    *
    *  
    * @return tuple, which has the constant term first and the slope second
    */
    def leastSquares(df: DataFrame):(Double, Double) = {
      import org.apache.spark.ml.regression.LinearRegression
      import org.apache.spark.ml.feature.VectorAssembler
      import org.apache.spark.ml.linalg.Vector

      val train_data = spark.sql("select DepDelay as label, avg(WeatherDelay) as feature from airtraffic where DepDelay>=0 group by DepDelay")

      val assembler = new VectorAssembler().
        setInputCols(Array("feature")).
        setOutputCol("features")

      val transformed = assembler.transform(train_data)
      val final_data = transformed.select("label","features")

      val model = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val model_fit = model.fit(final_data)

      (model_fit.intercept,model_fit.coefficients(0))
    }

    /**
    * Calculates the running average for DepDelay per day.
    * Average should be taken from both sides of the day we
    * are calculating the running average. You should take
    * 5 days from both sides of the day you are counting the running
    * average into consideration. For instance
    * if we want to calculate the average for the 10th of
    * January we have to take days: 5,6,7,8,9,10,11,12,13,14 and
    * 15 into account.
    *
    * Complete example
    * Let's assume that data looks like this
    *
    * +----+-----+---+--------+
    * |Year|Month|Day|DepDelay|
    * +----+-----+---+--------+
    * |2008|    3| 27|      12|
    * |2008|    3| 27|      -2|
    * |2008|    3| 28|       3|
    * |2008|    3| 29|      -5|
    * |2008|    3| 29|      12|
    * |2008|    3| 30|       5|
    * |2008|    3| 31|      47|
    * |2008|    4|  1|      45|
    * |2008|    4|  1|       2|
    * |2008|    4|  2|      -6|
    * |2008|    4|  3|       0|
    * |2008|    4|  3|       4|
    * |2008|    4|  3|      -2|
    * |2008|    4|  4|       2|
    * |2008|    4|  5|      27|
    * +----+-----+---+--------+
    *
    *
    * When running average is calculated
    * +----------+------------------+
    * |      date|    moving_average|
    * +----------+------------------+
    * |2008-03-27|13.222222222222221|
    * |2008-03-28|              11.3|
    * |2008-03-29| 8.846153846153847|
    * |2008-03-30| 8.357142857142858|
    * |2008-03-31|               9.6|
    * |2008-04-01|               9.6|
    * |2008-04-02|10.307692307692308|
    * |2008-04-03|10.916666666666666|
    * |2008-04-04|              12.4|
    * |2008-04-05|13.222222222222221|
    * +----------+------------------+
    *
    * For instance running_average for 
    * date 2008-04-03 => (-5+12+5+47+45+2-6+0+4-2+2+27)/12
    * =10.916666
    *
    *
    * @return DataFrame with schema 'date' (YYYY-MM-DD) (string type) and 'moving_average' (double type)
    * ordered by 'date'
    */
    def runningAverage(df: DataFrame): DataFrame = {
      val init = spark.sql("select Year, Month, DayofMonth, sum(DepDelay) as total, count(DepDelay) as count from airtraffic group by Year, Month, DayofMonth order by Year, Month, DayofMonth")
      init.createOrReplaceTempView("init")
      val format = spark.sql("select concat(Year,'-',Month,'-',DayofMonth) as date, total, count from init")
      format.createOrReplaceTempView("format")
      val dates = spark.sql("SELECT TO_DATE(CAST(UNIX_TIMESTAMP(date, 'yyyy-MM-dd') AS TIMESTAMP)) AS date, total, count from format")
      dates.createOrReplaceTempView("dates")
      val base = spark.sql("select date, total, count, sum(total) over (rows between 5 preceding and 5 following) as sum_total, sum(count) over (rows between 5 preceding and 5 following) as sum_count from dates")
      base.createOrReplaceTempView("base")
      spark.sql("select date, sum_total/sum_count as moving_average from base")
    }



}

/**
*
*  Change the student id
*/
object AirTrafficProcessor {
    val studentId = "361150"
}
