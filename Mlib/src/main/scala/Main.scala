package questions

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object Main {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val processor = new GamingProcessor()
    
    // load the data and tranform it
    val data = processor.convert(getClass().getResource("/osge_pool-1-thread-1.data").toString.drop(5))
    val indexed = processor.indexer(data)

    val assembled = processor.featureAssembler(indexed)

    val scaled = processor.scaler(assembled,"scaledFeatures")

    //split the dataset into training(90%) and prediction(10%) sets
    val splitted = scaled.randomSplit(Array(0.9,0.1), 10L)

    //train the model
    val model = processor.createModel(splitted(0),"scaledFeatures","paid_customer","prediction")

    //predict
    val predictions = processor.predict(model, splitted(1))

    //calculate how many correct predictions
    
    val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("paid_customer")
    .setRawPredictionCol("prediction")

    val res = evaluator.evaluate(predictions)
    
    
    println(Console.YELLOW+"Predicted correctly: " +Console.GREEN+ res*100 + "%")
    
    //finally stop spark
    processor.spark.stop()
  }
}


import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.{LogisticRegression,LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, IntegerType, DoubleType}
import spark.implicits._
import org.apache.spark.ml.feature.StringIndexer

val spark = SparkSession.builder.master("local").appName("gaming").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").getOrCreate()

val path = "src/main/resources/osge_pool-1-thread-1.data"

val customSchema = StructType(Array(
  StructField("cid", StringType,true),
  StructField("cname", StringType,true),
  StructField("email", StringType,true),
  StructField("gender_str", StringType,true),
  StructField("age", DoubleType,true),
  StructField("address", StringType,true),
  StructField("country", StringType,true),
  StructField("register_date", LongType,true),
  StructField("friend_count", DoubleType,true),
  StructField("lifetime", DoubleType,true),
  StructField("game1", DoubleType,true),
  StructField("game2", DoubleType,true),
  StructField("game3", DoubleType,true),
  StructField("game4", DoubleType,true),
  StructField("revenue", DoubleType,true),
  StructField("paid_customer_str", StringType,true)))

val data = spark.read.option("header","false").schema(customSchema).csv(path)

val indexed = indexer(data)

val assembled = featureAssembler(indexed)

val scaled = scaler(assembled,"scaledFeatures")

//split the dataset into training(90%) and prediction(10%) sets
val splitted = scaled.randomSplit(Array(0.01,0.001), 10L)

//train the model
val model = createModel(splitted(0),"scaledFeatures","paid_customer","prediction")

//predict
val predictions = predict(model, splitted(1))














