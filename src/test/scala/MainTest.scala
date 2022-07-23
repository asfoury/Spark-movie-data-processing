import app.aggregator.{Aggregator, RatingsLoader}
import app.recommender.{LSHIndex, NNLookup, NNLookupWithCache, TitlesLoader}
import org.apache.spark.sql.SparkSession
import org.junit.Test

import java.io.File


class MainTest {
  val master = "local[*]"
  val spark = SparkSession.builder.appName("Project2").master(master).getOrCreate
  val sc = spark.sparkContext

  @Test
  def testLoadRatingsSmall() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")

    val expected = Array(
      "(142,1982722,None,4.0,1000000000)",
      "(147,1889101,None,5.0,1000000000)",
      "(148,1889101,None,5.0,1000000000)",
      "(253,2388429,None,5.0,1000000000)",
      "(401,2310575,None,4.0,1000000000)"
    )

    val res = ratingsLoader
      .load()
      .collect()
      .sortWith((t1, t2) => t1.toString() <= t2.toString())

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))
  }

  @Test
  def testLoadTitlesSmall() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val expected = Array(
      "(1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))",
      "(1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))",
      "(2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))",
      "(2310622,Star Wars: The Clone Wars,List(jedi, fantasy))",
      "(2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))",
      "(2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy))",
      "(2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))"
    )

    val res = titlesLoader
      .load()
      .collect()
      .sortWith((t1, t2) => t1.toString() <= t2.toString())

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))
  }

  @Test
  def testSmallAggregation() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val res = aggregator
      .getResult()
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())

    val expected = Array(
      "(Fight Club,5.0)",
      "(Inception,4.0)",
      "(Star Wars: Attack of the Clones,4.0)",
      "(Star Wars: The Clone Wars,0.0)",
      "(Star Wars: The Empire Strikes Back,0.0)",
      "(The Lord of the Rings: The Fellowship of the Ring,0.0)",
      "(The Lord of the Rings: The Return of the King,5.0)"

    )

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))
  }

  @Test
  def testKeywordAggregation() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    assert(aggregator.getKeywordQueryResult(List("fantasy")) == 4.5)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "jedi")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "ring")) == 5.0)
    assert(aggregator.getKeywordQueryResult(List("scifi")) == -1.0)
  }

  @Test
  def testAggregatorUpdate() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val delta = Array((555, 2388426, None.asInstanceOf[Option[Double]], 4.0, 1000000001))

    aggregator.updateResult(delta)

    val res = aggregator
      .getResult()
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())

    val expected = Array(
      "(Fight Club,5.0)",
      "(Inception,4.0)",
      "(Star Wars: Attack of the Clones,4.0)",
      "(Star Wars: The Clone Wars,0.0)",
      "(Star Wars: The Empire Strikes Back,0.0)",
      "(The Lord of the Rings: The Fellowship of the Ring,4.0)",
      "(The Lord of the Rings: The Return of the King,5.0)"

    )

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))

    assert((aggregator.getKeywordQueryResult(List("fantasy")) - 4.333).abs < 0.01)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "jedi")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "ring")) == 4.5)
  }

  @Test
  def testAggregatorUpdate2() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val delta = Array((555, 2388429, Option[Double](5.0), 4.0, 1000000001))

    aggregator.updateResult(delta)

    val res = aggregator
      .getResult()
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())

    val expected = Array(
      "(Fight Club,5.0)",
      "(Inception,4.0)",
      "(Star Wars: Attack of the Clones,4.0)",
      "(Star Wars: The Clone Wars,0.0)",
      "(Star Wars: The Empire Strikes Back,0.0)",
      "(The Lord of the Rings: The Fellowship of the Ring,0.0)",
      "(The Lord of the Rings: The Return of the King,4.0)"

    )

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))

    assert(aggregator.getKeywordQueryResult(List("fantasy")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "jedi")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "ring")) == 4.0)
  }

  @Test
  def testAggregatorUpdate3() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val delta1 = Array((555, 2388429, Option[Double](5.0), 4.0, 1000000001))

    aggregator.updateResult(delta1)

    val delta2 = Array((555, 2388429, Option[Double](4.0), 5.0, 1000000002))

    aggregator.updateResult(delta2)

    val res = aggregator
      .getResult()
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())

    val expected = Array(
      "(Fight Club,5.0)",
      "(Inception,4.0)",
      "(Star Wars: Attack of the Clones,4.0)",
      "(Star Wars: The Clone Wars,0.0)",
      "(Star Wars: The Empire Strikes Back,0.0)",
      "(The Lord of the Rings: The Fellowship of the Ring,0.0)",
      "(The Lord of the Rings: The Return of the King,5.0)"

    )

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))

    assert(aggregator.getKeywordQueryResult(List("fantasy")) == 4.5)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "jedi")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "ring")) == 5.0)
  }

  @Test
  def testAggregatorUpdate4() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-small.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val delta1 = Array((555, 2388426, None.asInstanceOf[Option[Double]], 5.0, 1000000001))

    aggregator.updateResult(delta1)

    val delta2 = Array((555, 2388429, Option[Double](5.0), 4.0, 1000000002))

    aggregator.updateResult(delta2)

    val res = aggregator
      .getResult()
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())

    val expected = Array(
      "(Fight Club,5.0)",
      "(Inception,4.0)",
      "(Star Wars: Attack of the Clones,4.0)",
      "(Star Wars: The Clone Wars,0.0)",
      "(Star Wars: The Empire Strikes Back,0.0)",
      "(The Lord of the Rings: The Fellowship of the Ring,5.0)",
      "(The Lord of the Rings: The Return of the King,4.0)"

    )

    res.zip(expected).foreach(t=> assert(t._1.toString() == t._2))

    assert((aggregator.getKeywordQueryResult(List("fantasy")) - 4.333).abs < 0.01)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "jedi")) == 4.0)
    assert(aggregator.getKeywordQueryResult(List("fantasy", "ring")) == 4.5)
  }

  @Test
  def testBuildLSH() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val lsh = new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16))

    val res = lsh
      .getBuckets()
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val expected = Array(
      "(Vector(116205, 68148),List((1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))))",
      "(Vector(20094, 80509),List((1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))))",
      "(Vector(31000, 22057),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(Vector(68631, 3177),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(Vector(8052, 3177),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))"
    )

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testSelfJoin() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val lsh = new NNLookup(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = titlesLoader
      .load()
      .map(x => x._3)

    val res = lsh
      .lookup(queries)
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val expected = Array(
      "(List(fantasy-becomes-reality, inside-the-mind),List((1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, political-manipulation, fantasy),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))",
      "(List(troll, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(troll, tentacle, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head),List((1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))))"
    )

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testEmptyJoin() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val lsh = new NNLookup(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = sc.parallelize(List(List("space"), List("cowboy")))

    val res = lsh
      .lookup(queries)
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val expected = Array(
      "(List(cowboy),List())",
      "(List(space),List())"
    )

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testSelfJoinCacheEmpty() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val lsh = new NNLookupWithCache(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = titlesLoader
      .load()
      .map(x => x._3)

    val cres = lsh
      .cacheLookup(queries)

    assert(cres._1 == null)
    assert(cres._2.collect().length != 0)

    val cexpected = Array(
      "(Vector(116205, 68148),List(fantasy-becomes-reality, inside-the-mind))",
      "(Vector(20094, 80509),List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))",
      "(Vector(31000, 22057),List(troll, ring, fantasy))",
      "(Vector(31000, 22057),List(troll, tentacle, ring, fantasy))",
      "(Vector(68631, 3177),List(jedi, fantasy))",
      "(Vector(68631, 3177),List(jedi, fantasy))",
      "(Vector(8052, 3177),List(jedi, political-manipulation, fantasy))"
    )

    cres._2
      .collect()
      .sortWith((a, b) => a.toString() <= b.toString())
      .zip(cexpected)
      .foreach(t => assert(t._1.toString() == t._2))

    val res = lsh
      .lookup(queries)
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val expected = Array(
      "(List(fantasy-becomes-reality, inside-the-mind),List((1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, political-manipulation, fantasy),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))",
      "(List(troll, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(troll, tentacle, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head),List((1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))))"
    )

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testSelfJoinCacheFull() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val full = Map[IndexedSeq[Int], List[(Int, String, List[String])]](
      (IndexedSeq(116205, 68148),List((1982722,"Inception",List("fantasy-becomes-reality", "inside-the-mind")))),
      (IndexedSeq(20094, 80509),List((1889101,"Fight Club",List("urban-decay", "dark-humor", "plot-twist", "violence", "shot-in-the-head")))),
      (IndexedSeq(31000, 22057),List((2388426,"The Lord of the Rings: The Fellowship of the Ring",List("troll", "tentacle", "ring", "fantasy")), (2388429,"The Lord of the Rings: The Return of the King",List("troll", "ring", "fantasy")))),
      (IndexedSeq(68631, 3177),List((2310622,"Star Wars: The Clone Wars",List("jedi", "fantasy")), (2322446,"Star Wars: The Empire Strikes Back",List("jedi", "fantasy")))),
      (IndexedSeq(8052, 3177),List((2310575,"Star Wars: Attack of the Clones",List("jedi", "political-manipulation", "fantasy"))))
    )

    val lsh = new NNLookupWithCache(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = titlesLoader
      .load()
      .map(x => x._3)

    val cache_bcst = sc.broadcast(full)

    lsh.buildExternal(cache_bcst)

    val cres = lsh
      .cacheLookup(queries)

    assert(cres._1 != null)
    assert(cres._2.collect().length == 0)

    val res = lsh
      .lookup(queries)
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val expected = Array(
      "(List(fantasy-becomes-reality, inside-the-mind),List((1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, political-manipulation, fantasy),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))",
      "(List(troll, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(troll, tentacle, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head),List((1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))))"
    )

    cres._1
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())
      .zip(expected).foreach(t => assert(t._1.toString() == t._2))

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testSelfJoinCachePartial() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val partial = Map[IndexedSeq[Int], List[(Int, String, List[String])]](
      (IndexedSeq(31000, 22057),List((2388426,"The Lord of the Rings: The Fellowship of the Ring",List("troll", "tentacle", "ring", "fantasy")), (2388429,"The Lord of the Rings: The Return of the King",List("troll", "ring", "fantasy")))),
      (IndexedSeq(68631, 3177),List((2310622,"Star Wars: The Clone Wars",List("jedi", "fantasy")), (2322446,"Star Wars: The Empire Strikes Back",List("jedi", "fantasy")))),
      (IndexedSeq(8052, 3177),List((2310575,"Star Wars: Attack of the Clones",List("jedi", "political-manipulation", "fantasy"))))
    )

    val lsh = new NNLookupWithCache(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = titlesLoader
      .load()
      .map(x => x._3)

    val cache_bcst = sc.broadcast(partial)

    lsh.buildExternal(cache_bcst)

    val cres = lsh
      .cacheLookup(queries)

    assert(cres._1 != null)

    val res = lsh
      .lookup(queries)
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())

    val cexpected = Array(
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, political-manipulation, fantasy),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))",
      "(List(troll, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(troll, tentacle, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))"
    )

    val mexpected = Array(
      (IndexedSeq(116205, 68148),List("fantasy-becomes-reality", "inside-the-mind")),
      (IndexedSeq(20094, 80509),List("urban-decay", "dark-humor", "plot-twist", "violence", "shot-in-the-head"))
    )

    val expected = Array(
      "(List(fantasy-becomes-reality, inside-the-mind),List((1982722,Inception,List(fantasy-becomes-reality, inside-the-mind))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, fantasy),List((2310622,Star Wars: The Clone Wars,List(jedi, fantasy)), (2322446,Star Wars: The Empire Strikes Back,List(jedi, fantasy))))",
      "(List(jedi, political-manipulation, fantasy),List((2310575,Star Wars: Attack of the Clones,List(jedi, political-manipulation, fantasy))))",
      "(List(troll, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(troll, tentacle, ring, fantasy),List((2388426,The Lord of the Rings: The Fellowship of the Ring,List(troll, tentacle, ring, fantasy)), (2388429,The Lord of the Rings: The Return of the King,List(troll, ring, fantasy))))",
      "(List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head),List((1889101,Fight Club,List(urban-decay, dark-humor, plot-twist, violence, shot-in-the-head))))"
    )

    cres._1
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())
      .zip(cexpected).foreach(t => assert(t._1.toString() == t._2))

    cres._2
      .collect()
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())
      .zip(mexpected).foreach(t => assert(t._1.toString() == t._2.toString()))

    res.zip(expected).foreach(t => assert(t._1.toString() == t._2))
  }

  @Test
  def testSelfJoinCacheBuild() {
    val titlesLoader = new TitlesLoader(sc, "/titles-small.csv")

    val lsh = new NNLookupWithCache(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val tuneQueriesLocal =
      (1 to 100).map(x => List("jedi","fantasy")).toList ++ List(List("fantasy-becomes-reality","inside-the-mind"))

    val tuneQueries = sc.parallelize(tuneQueriesLocal)

    val res = lsh
      .lookup(tuneQueries)
      .collect()

    lsh.build(sc)

    val testQueriesLocal = List(
      List("jedi","fantasy"),
      List("fantasy-becomes-reality","inside-the-mind"),
      List("troll","ring","fantasy")
    )

    val testQueries = sc.parallelize(testQueriesLocal)

    val cres = lsh
      .cacheLookup(testQueries)

    assert(cres._1 != null)

    val cexpected = List(
      (List("jedi", "fantasy"),List((2310622,"Star Wars: The Clone Wars",List("jedi", "fantasy")), (2322446,"Star Wars: The Empire Strikes Back",List("jedi", "fantasy"))))
    )

    val mexpected = List(
      (IndexedSeq(116205, 68148),List("fantasy-becomes-reality", "inside-the-mind")),
      (IndexedSeq(31000, 22057),List("troll", "ring", "fantasy"))
    )

    cres
      ._1
      .collect()
      .map(t => (t._1, t._2.sortWith((t1, t2) => t1.toString() <= t2.toString())))
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())
      .zip(cexpected)
      .foreach(t => assert(t._1.toString() == t._2.toString()))

    cres._2
      .collect()
      .sortWith((t1, t2) => t1._1.toString() <= t2._1.toString())
      .zip(mexpected)
      .foreach(t => assert(t._1.toString() == t._2.toString()))
  }

  @Test
  def testMediumAggregation() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-medium.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-medium.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val res = aggregator
      .getResult()
      .map(x => (x._1, (x._2*1000).toInt))
      .map(x => (x.toString(), 1))
      .reduceByKey(_ + _)
      .map(t => (t.toString(), 1))

    val file = new File(getClass.getResource("/aggr-res.csv").getFile).getPath

    val expected = sc
      .textFile(file)
      .map(x => x.split('|'))
      .map(x => (x(0), (x(1).toDouble*1000).toInt))
      .map(x => (x.toString(), 1))
      .reduceByKey(_ + _)
      .map(t => (t.toString(), 1))

    assert(res.count() == res.join(expected).count())

    assert((aggregator.getKeywordQueryResult(List("independent-film")) - 2.9977).abs < 0.001)
    assert((aggregator.getKeywordQueryResult(List("cartoon")) - 3.1666).abs < 0.001)
  }

  @Test
  def testMediumUpdate() {
    val ratingsLoader = new RatingsLoader(sc, "/ratings-medium.csv")
    val titlesLoader = new TitlesLoader(sc, "/titles-medium.csv")

    val aggregator = new Aggregator(sc)

    aggregator.init(ratingsLoader.load(), titlesLoader.load())

    val deltafile = new File(getClass.getResource("/ratings-medium-delta.csv").getFile).getPath

    val delta = sc
      .textFile(deltafile)
      .map(x => x.split('|'))
      .map(x => (x(0).toInt, x(1).toInt, x(2).toDouble, x(3).toDouble, x(4).toInt))
      .map(x =>
        if (x._3 < 0)
          (x._1, x._2, Option.empty[Double], x._4, x._5)
        else
          (x._1, x._2, Option[Double](x._3), x._4, x._5)
      ).collect()

    aggregator.updateResult(delta)

    val res = aggregator
      .getResult()
      .map(x => (x._1, (x._2*1000).toInt))
      .map(x => (x.toString(), 1))
      .reduceByKey(_ + _)
      .map(t => (t.toString(), 1))

    val file = new File(getClass.getResource("/aggr-res2.csv").getFile).getPath

    val expected = sc
      .textFile(file)
      .map(x => x.split('|'))
      .map(x => (x(0), (x(1).toDouble*1000).toInt))
      .map(x => (x.toString(), 1))
      .reduceByKey(_ + _)
      .map(t => (t.toString(), 1))

    assert(res.count() == res.join(expected).count())

    assert((aggregator.getKeywordQueryResult(List("independent-film")) - 2.99378).abs < 0.001)
    assert((aggregator.getKeywordQueryResult(List("cartoon")) - 3.21999).abs < 0.001)
  }

  @Test
  def testMediumSelfJoin() {
    val titlesLoader = new TitlesLoader(sc, "/titles-medium.csv")

    val lsh = new NNLookup(new LSHIndex(titlesLoader.load(), IndexedSeq(5, 16)))

    val queries = titlesLoader
      .load()
      .map(x => x._3)

    val res = lsh
      .lookup(queries)
      .filter(x => x._2.filter(y =>
        x._1.size == y._3.size && x._1.zip(y._3).filter(a => a._1 != a._2).size == 0
      ).size != 0)

    assert(res.count() == titlesLoader.load().count())
  }
}
