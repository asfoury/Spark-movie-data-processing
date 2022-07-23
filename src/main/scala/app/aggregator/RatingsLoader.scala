package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val file = new File(getClass.getResource(path).getFile).getPath
    val ratingsRDD : RDD[String] = sc.textFile(file)
     val res = ratingsRDD.map{
      lineString => {
        // need to escape the | char because of regex
        val lineArray = lineString.split("\\|")
        val userID : Int = lineArray(0).toInt
        val titleID : Int  = lineArray(1).toInt
        val oldRating : Option[Double] = None
        val newRating : Double = lineArray(2).toDouble
        val timeStamp : Int = lineArray(3).toInt
        (userID, titleID, oldRating, newRating, timeStamp)
      }
    }
    // keep the RDD in memory
    res.cache()
  }
}
