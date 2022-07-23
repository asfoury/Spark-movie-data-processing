package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File
import scala.collection.mutable.ListBuffer

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class TitlesLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val file = new File(getClass.getResource(path).getFile).getPath
    val titlesRDD : RDD[String] = sc.textFile(file)
    val res = titlesRDD.map{
      lineString => {
        // need to escape the | char because of regex
        val lineArray : Array[String] = lineString.split("\\|")
        val titleID : Int = lineArray(0).toInt
        val titleName : String  = lineArray(1)
        val keyWords = lineArray.tail.tail.foldLeft(ListBuffer[String]())(_ += _)
        (titleID, titleName, keyWords.toList)
      }
    }
    // keep the RDD in memory
    res.cache()
  }
}
