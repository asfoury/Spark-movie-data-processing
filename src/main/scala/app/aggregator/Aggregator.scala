package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc : SparkContext) extends Serializable {

  var state : Option[RDD[(Int, (String, List[String], Double, Int))]] = None

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings : RDD[(Int, Int, Option[Double], Double, Int)],
            title : RDD[(Int, String, List[String])]
          ) : Unit = {
    // the avg and the number are put to 0.0 and 0 so they do not participate in the rating calculation if title not rated shows up as
    // -> (titleId, (<name>, <keyword>, 0.0, 0))
    val allTitles = title.map(tup => (tup._1, (0.0, 0, tup._3, tup._2)))
    // the avg is put to the avg and number to 1 so that they count towards the avg
    val ratedTitles = ratings.map(rating => (rating._2, (rating._4, 1, List[String](), "")))

    // RDD[(titleID, (titleName, titleKeyWords, sumOfRatings, numberOfRatings))]
    val combined = allTitles.union(ratedTitles).reduceByKey{
      (tup1, tup2) => {
        val(rating1, number1, keyWords1, title1) = tup1
        val(rating2, number2, keyWords2, title2) = tup2
        (rating1 + rating2, number1 + number2, keyWords1 ++ keyWords2, title1 + title2)
      }
    }.map { tup => {
      (tup._1, (tup._2._4, tup._2._3, tup._2._1, tup._2._2))
    }
    }.cache()
    this.state = Option(combined)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult() : RDD[(String, Double)] = {
    this.state match {
      case None => throw new Exception("Should run init before getResult")
      case Some(rdd) => rdd.map{ tup => {
        val (_, (titleName, _, sumOfRatings, numberOfRatings)) = tup
        val divRes = if(numberOfRatings != 0) sumOfRatings / numberOfRatings else 0.0
        (titleName, divRes)
      }}
    }
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords : List[String]) : Double = {
    this.state match {
      case None => throw new Exception("Should run init before getKeywordQueryResult")
      case Some(rdd) => {
        val tuplesThatContainKeyWord = rdd.filter(tup => {
          val (_, (_, titleKeyWords, _, _)) = tup
          keywords.forall(titleKeyWords.contains(_))
        })
        if(tuplesThatContainKeyWord.isEmpty()) return -1
        val combined = tuplesThatContainKeyWord.map(tup => {
          val (_, (_, _, sumOfRatings, numberOfRatings)) = tup
          if(numberOfRatings != 0)
            (sumOfRatings/numberOfRatings, 1)
          else
            (0.0, 0)
        }).reduce((tup1, tup2) => {
          (tup1._1 + tup2._1, tup1._2 + tup2._2)
        })
        if(combined._2 == 0) return 0.0
        combined._1 / combined._2
      }
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]) : Unit = {
    this.state match {
      case None => throw new Exception("Should run init before updateResult")
      case Some(rdd) => {
        val deltaMapped = delta_.map(newRating => {
          if(newRating._3.isDefined) {
            // this is a rating being changed
            val oldValue = newRating._3.get
            val newValue = newRating._4
            (newRating._2, ("", List[String](), newValue - oldValue, 0))
          } else {
            // this is a totally new rating
            val newValue = newRating._4
            (newRating._2, ("", List[String](), newValue, 1))
          }

        })
        val newRatingsFormatted = sc.parallelize(deltaMapped)
        val rddMapped = rdd.map(tup => (tup._1,(tup._2._1, tup._2._2, tup._2._3, tup._2._4)))
        val combined = rddMapped.union(newRatingsFormatted)
        val red = combined.reduceByKey{ (movieRat1, movieRat2) => {
          val (titleName1, keyWords1, sumOfRatings1, numberOfRatings1) = movieRat1
          val (titleName2, keyWords2, sumOfRatings2, numberOfRatings2) = movieRat2
          (titleName1 + titleName2, keyWords1 ++ keyWords2, sumOfRatings1 + sumOfRatings2, numberOfRatings1 + numberOfRatings2)
        }
        }
        rdd.unpersist()
        red.cache()
        this.state = Some(red)
        }
      }
    }
}
