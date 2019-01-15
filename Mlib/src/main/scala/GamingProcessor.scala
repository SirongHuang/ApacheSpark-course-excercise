package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.{LogisticRegression,LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,StringType, DoubleType, LongType}


/**
* GamingProcessor is used to predict if the user is a subscriber.
* You can find the data files from /resources/gaming_data.
* Data schema is https://github.com/cloudwicklabs/generator/wiki/OSGE-Schema
* (first table)
*
* Use Spark's machine learning library mllib's logistic regression algorithm.
* https://spark.apache.org/docs/latest/ml-classification-regression.html#binomial-logistic-regression
*
* Use these features for training your model:
*   - gender 
*   - age
*   - country
*   - friend_count
*   - lifetime
*   - citygame_played
*   - pictionarygame_played
*   - scramblegame_played
*   - snipergame_played
*
*   -paid_subscriber(this is the feature to predict)
*
* The data contains categorical features, so you need to
* change them accordingly.
* https://spark.apache.org/docs/latest/ml-features.html
*
*
*
*/
class GamingProcessor() {


  // these parameters can be changed
  val spark = SparkSession.builder
  .master("local")
  .appName("gaming")
  .config("spark.driver.memory", "5g")
  .config("spark.executor.memory", "2g")
  .getOrCreate()

  import spark.implicits._


  /**
  * convert creates a dataframe, removes unnecessary colums and converts the rest to right format. 
  * Data schema:
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *
  * @param path to file
  * @return converted DataFrame
  */
  def convert(path: String): DataFrame = {

    import org.apache.spark.sql.functions.{col, when}

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

    val data = spark.read
      .option("header","false")
      .schema(customSchema)
      .csv(path)

    val result = data
      .withColumn("gender", when($"gender_str"==="male", 1.0).otherwise(0.0))
      .withColumn("paid_customer",when($"paid_customer_str"==="yes", 1.0).otherwise(0.0))
      .select("gender","age","country","friend_count","lifetime","game1","game2","game3","game4","paid_customer")

    return result

  }

  /**
  * indexer converts categorical features into doubles.
  * https://spark.apache.org/docs/latest/ml-features.html
  * 'country' is the only categorical feature.
  * After these modifications schema should be:
  *
  *   - gender: Double (1 if male else 0)
  *   - age: Double
  *   - country: String
  *   - friend_count: Double
  *   - lifetime: Double
  *   - game1: Double (citygame_played)
  *   - game2: Double (pictionarygame_played)
  *   - game3: Double (scramblegame_played)
  *   - game4: Double (snipergame_played)
  *   - paid_customer: Double (1 if yes else 0)
  *   - country_index: Double
  *
  * @param df DataFrame
  * @return Dataframe
  */
  def indexer(df: DataFrame): DataFrame = {

    val stringindexer = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("country_index")

    val result = stringindexer.fit(df).transform(df)
    return result
  }

  /**
  * Combine features into one vector. Most mllib algorithms require this step
  * https://spark.apache.org/docs/latest/ml-features.html#vectorassembler
  * Column name should be 'features'
  *
  * @param Dataframe that is transformed using indexer
  * @return Dataframe
  */
  def featureAssembler(df: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("gender","age","country_index","friend_count","lifetime","game1","game2","game3","game4"))
      .setOutputCol("features")

    val output = assembler.transform(df)
    return output
  }

  /**
  * To improve performance data also need to be standardized, so use
  * https://spark.apache.org/docs/latest/ml-features.html#standardscaler
  *
  * @param Dataframe that is transformed using featureAssembler
  * @param  name of the scaled feature vector (output column name)
  * @return Dataframe
  */
  def scaler(df: DataFrame, outputColName: String): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol(outputColName)
      .setWithStd(true)
      .setWithMean(true)

    val result = scaler.fit(df).transform(df)
    return result
  }


  /**
  * createModel creates a logistic regression model
  * When training, 5 iterations should be enough.
  *
  * @param transformed dataframe
  * @param featuresCol name of the features columns
  * @param labelCol name of the label col (paid_customer)
  * @param predCol name of the prediction col
  * @return trained LogisticRegressionModel
  */
  def createModel(training: DataFrame, featuresCol: String, labelCol: String, predCol: String): LogisticRegressionModel = {

    val lr = new LogisticRegression()
      .setMaxIter(5)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setLabelCol(labelCol)
      .setFeaturesCol(featuresCol)
      .setPredictionCol(predCol)

    val lrModel = lr.fit(training)
    return lrModel

  } 


  /**
  * Given a transformed and normalized dataset
  * this method predicts if the customer is going to
  * subscribe to the service.
  *
  * @param model trained logistic regression model
  * @param dataToPredict normalized data for prediction
  * @return DataFrame predicted scores (1.0 == yes, 0.0 == no)
  */
  def predict(model: LogisticRegressionModel, dataToPredict: DataFrame): DataFrame = {
    val predictions = model.transform(dataToPredict)
    return predictions
  }

}
/**
*
*  Change the student id
*/
object GamingProcessor {
    val studentId = "361150"
}
