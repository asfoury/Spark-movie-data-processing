package app.recommender

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Class for performing LSH lookups (enhanced with cache)
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookupWithCache(lshIndex : LSHIndex) extends Serializable {
  var cache : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]] = null
  var book: mutable.Map[IndexedSeq[Int], Int] = collection.mutable.Map[IndexedSeq[Int], Int]()

  /**
   * The operation for building the cache
   *
   * @param sc Spark context for current application
   */
  def build(sc : SparkContext) = {
      val nbQueries = this.book.values.sum
      val filtered = this.book.filter(tup => tup._2 / nbQueries > 0.01)
      val bookRdd = sc.parallelize(filtered.toSeq)
      val queriesInCache = lshIndex.lookup(bookRdd).map(tup => (tup._1, tup._3))
      this.cache = sc.broadcast(queriesInCache.collect().toMap)
    // reset the bookkeeping
    this.book = collection.mutable.Map[IndexedSeq[Int], Int]()
  }

  /**
   * Testing operation: force a cache based on the given object
   *
   * @param ext A broadcast map that contains the objects to cache
   */
  def buildExternal(ext : Broadcast[Map[IndexedSeq[Int], List[(Int, String, List[String])]]]) = {
      this.cache = ext
  }


  /**
   * Lookup operation on cache
   *
   * @param queries The RDD of keyword lists
   * @return The pair of two RDDs
   *         The first RDD corresponds to queries that result in cache hits and
   *         includes the LSH results
   *         The second RDD corresponds to queries that result in cache hits and
   *         need to be directed to LSH
   */
  def cacheLookup(queries: RDD[List[String]])
  : (RDD[(List[String], List[(Int, String, List[String])])], RDD[(IndexedSeq[Int], List[String])]) = {
    val hashedQueries = lshIndex.hash(queries)
    // do the bookkeeping
      hashedQueries.groupBy(_._1).map(tup => (tup._1, tup._2.size)).collect().toMap.foreach(tup => {
        if (this.book.contains(tup._1)) {
          val old = this.book(tup._1)
          this.book.update(tup._1, old + tup._2)
        } else {
          this.book.put(tup._1, tup._2)
        }
      })
    if(this.cache != null) {
      val hits = hashedQueries.filter( query => this.cache.value.contains(query._1)).map(tup => (tup._2, this.cache.value(tup._1)))
      val misses =  hashedQueries.filter( query => !this.cache.value.contains(query._1))
      (hits, misses)
    } else {
      val missed = lshIndex.hash(queries)
      (null, missed)
    }

  }

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    val (hits, misses) = cacheLookup(queries)
    val fetched =  lshIndex.lookup(misses).map(tup => (tup._2, tup._3))
    if(hits != null)
     hits.union(fetched)
    else
      fetched
  }
}
