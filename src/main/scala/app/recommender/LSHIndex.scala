package app.recommender

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {
  private val minhash = new MinHash(seed)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])] = {
    input.map(x => (minhash.hash(x), x))
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets()
    : RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
      val keyWordsRDD = this.data.map(_._3)
      val hashed = hash(keyWordsRDD).map(tup => (tup._2, (tup._1, List[(Int, String, List[String])]())))
      val tem = this.data.map(tup => (tup._3,(IndexedSeq[Int](),List(tup))))
      val combined = hashed.union(tem).reduceByKey{ (lhs, rhs) => {
        val hash = if(lhs._1.isEmpty) rhs._1 else lhs._1
        (hash, lhs._2 ++ rhs._2)
      }
      }
    combined.map(tup => tup._2).groupByKey().map( tup => (tup._1.toIndexedSeq, tup._2.flatten.toList))
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    val buckets = getBuckets()
    val joined = buckets.join(queries).map(tup => (tup._1, tup._2._2, tup._2._1))
    joined
  }
}
