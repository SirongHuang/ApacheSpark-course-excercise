package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import scala.math._

import org.apache.spark.graphx._
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph


/** GeoProcessor provides functionalites to 
* process country/city/location data.
* We are using data from http://download.geonames.org/export/dump/
* which is licensed under creative commons 3.0
  * http://creativecommons.org/licenses/by/3.0/
*
* @param spark reference to SparkSession 
* @param filePath path to file that should be modified
*/
class GeoProcessor(spark: SparkSession, filePath:String) {

    //read the file and create an RDD
    //DO NOT EDIT
    val file = spark.sparkContext.textFile(filePath)

    /** filterData removes unnecessary fields and splits the data so
    * that the RDD looks like RDD(Array("<name>","<countryCode>","<dem>"),...))
    * Fields to include:
    *   - name
    *   - countryCode
    *   - dem (digital elevation model)
    *
    * @return RDD containing filtered location data. There should be an Array
      *         for each location
    */
    def filterData(data: RDD[String]): RDD[Array[String]] = {
        /* hint: you can first split each line into an array.
        * Columns are separated by tab ('\t') character. 
        * Finally you should take the appropriate fields.
        * Function zipWithIndex might be useful.
        */

        data.map(line => {
            val columns = line.split("\t")
            Array(columns(1), columns(8), columns(16))
        })
    }



    /** filterElevation is used to filter to given countryCode
    * and return RDD containing only elevation(dem) information
    *
    * @param countryCode code e.g(AD)
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD containing only elevation information
    */
    def filterElevation(countryCode: String, data: RDD[Array[String]]): RDD[Int] = {

        val evaluated = data.filter(x => x(1) == countryCode)
        evaluated.map(x => x(2).toInt)
    }


    /** elevationAverage calculates the elevation(dem) average
    * to specific dataset.
    *
    * @param data: RDD containing only elevation information
    * @return The average elevation
    */
    def elevationAverage(data: RDD[Int]): Double = {

        data.map(x => x.toDouble)
        data.sum / data.count

    }


    /** mostCommonWords calculates what is the most common 
    * word in place names and returns an RDD[(String,Int)]
    * You can assume that words are separated by a single space ' '.
    *
    * @param data an RDD containing multiple Array[<name>, <countryCode>, <dem>]
    * @return RDD[(String,Int)] where string is the word and Int number of
    * occurrences. RDD should be in descending order (sorted by number of
      * occurrences).
    * e.g ("hotel", 234), ("airport", 120), ("new", 12)
    */
    def mostCommonWords(data: RDD[Array[String]]): RDD[(String,Int)] = {

        val names = data.flatMap(line => line(0).split(" ")).map(word=>(word,1))
        val word_count = names.reduceByKey(_+_)
        word_count.map(x=>x.swap).sortByKey(false).map(x=>x.swap)

    }

    /** mostCommonCountry tells which country has the most
    * entries in geolocation data. The correct name for specific
    * countrycode can be found from countrycodes.csv.
    *
    * @param data filtered geoLocation data
    * @param path to countrycode.csv file
    * @return most common country as String e.g Finland or empty string ""
      *         if countrycodes.csv
    *         doesn't have that entry.
    */
    def mostCommonCountry(data: RDD[Array[String]], path: String): String = {
        val country_code = spark.sparkContext.textFile(path).map(x => x.split(","))
        val country_count = data.map(x=>x(1)).map(x=>(x,1)).reduceByKey(_+_).map(x=>x.swap).sortByKey(false)
        val most_common = country_count.first._2
        val checker = country_code.filter(x=>x(1).contains(most_common)).collect
        if (checker.isEmpty) {"";
        } else {country_code.filter(x=>x(1).contains(most_common)).map(x=>x(0)).first}
    }





    /**
    * How many hotels are within 10 km (<=10000.0) from
    * given latitude and longitude?
    * https://en.wikipedia.org/wiki/Haversine_formula
    * earth radius is 6371e3 meters.
    *
    * Location is a hotel if the name contains the word 'hotel'.
    * Don't use feature code field!
    *
    * Important
    *   if you want to use helper functions, use variables as
    *   functions, e.g
    *   val distance = (a: Double) => {...}
    *
    * @param lat latitude as Double
    * @param long longitude as Double
    * @return number of hotels in area
    */
    def hotelsInArea(lat: Double, long: Double): Int = {

        val haversine_distance = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
            val R = 6371e3
            val dLat = (lat2 - lat1).toRadians
            val dLon = (lon2 - lon1).toRadians

            val a = pow(sin(dLat / 2), 2) + pow(sin(dLon / 2), 2) * cos(lat1.toRadians) * cos(lat2.toRadians)
            val c = 2 * asin(sqrt(a))
            R * c
        }

        val data = file.map(line=>{
            val col = line.split("\t")
            Array(col(1), col(4), col(5))
        })

        val hotels = data.filter(x=>x(0).toLowerCase.contains("hotel"))

        val hotels_nearby = hotels.filter(x=>haversine_distance(lat,long, x(1).toDouble,x(2).toDouble)<=10000.0)

        hotels_nearby.count.toInt

    }

    //GraphX exercises

    /**
    * Load FourSquare social graph data, create a
    * graphx graph and return it.
    * Use user id as vertex id and vertex attribute.
    * Use number of unique connections between users as edge weight.
    * E.g
    * ---------------------
    * | user_id | dest_id |
    * ---------------------
    * |    1    |    2    |
    * |    1    |    2    |
    * |    2    |    1    |
    * |    1    |    3    |
    * |    2    |    3    |
    * ---------------------
    *         || ||
    *         || ||
    *         \   /
    *          \ /
    *           +
    *
    *         _ 3 _
    *         /' '\
    *        (1)  (1)
    *        /      \
    *       1--(2)--->2
    *        \       /
    *         \-(1)-/
    *
    * Hints:
    *  - Regex is extremely useful when parsing the data in this case.
    *  - http://spark.apache.org/docs/latest/graphx-programming-guide.html
    *
    * @param path to file. You can find the dataset
    *  from the resources folder
    * @return graphx graph
    *
    */
    def loadSocial(path: String): Graph[Int,Int] = {

        //*** processing data
        val file = spark.sparkContext.textFile(path)
        val pattern = "[0-9]+".r
        // only contain rows with 2 numbers
        val raw = file.filter(x=>{
            val numbers = pattern.findAllIn(x)
            numbers.length==2})
        // raw string => (Int, Int)
        val data = raw.map(x=>pattern.findAllIn(x).toList).map(x=>(x(0).toInt, x(1).toInt)).sortByKey()

        //*** vertices
        // all users (flatten links between all users)
        val all_users = data.map(x=>x._1) ++ data.map(x=>x._2)
        // unique users
        val users = all_users.map(x=>(x,x)).distinct.sortByKey()
        val vertices: RDD[(VertexId, Int)] = users.map(x=>(x._1.toLong,(x._2)))

        //*** edges
        val relationship_count = data.map(x=>((x._1.toLong,x._2.toLong), 1)).groupByKey().map(x=>(x._1,x._2.toList.sum))
        val edges: RDD[Edge[Int]] = relationship_count.map(x=>Edge(x._1._1,x._1._2, x._2))

        Graph(vertices, edges)
    }


    /**
    * Which user has the most outward connections.
    *
    * @param graph graphx graph containing the data
    * @return vertex_id as Int
    */
    def mostActiveUser(graph: Graph[Int,Int]): Int = {
        // define custom max count finder for type (VertexId, Int)
        val max = (a: (VertexId, Int), b: (VertexId, Int)) => {
            if (a._2 > b._2) a else b}
        graph.outDegrees.reduce(max)._1.toInt
    }

    /**
    * Which user has the highest pageRank.
    * https://en.wikipedia.org/wiki/PageRank
    *
    * @param graph graphx graph containing the data
    * @return user with highest pageRank
    */
    def pageRankHighest(graph: Graph[Int,Int]): Int = {
        val pageRanks = graph.pageRank(0.0001).vertices
        pageRanks.map(x=>x.swap).sortByKey(false).first._2.toInt
    }
}
/**
*
*  Change the student id
*/
object GeoProcessor {
    val studentId = "361150"
}