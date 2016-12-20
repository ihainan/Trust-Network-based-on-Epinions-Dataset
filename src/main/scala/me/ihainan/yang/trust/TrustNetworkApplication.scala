package me.ihainan.yang.trust

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Trust Network Application
  */
object TrustNetworkApplication {
  // sample fraction of rating.txt file
  val RATING_FILE_SAMPLING_FRACTION = 0.001
  // sample fraction of mc.txt file
  val MC_FILE_SAMPLING_FRACTION = 0.001
  // sample fraction of user_rating.txt file
  val USER_RATING_FILE_SAMPLING_FRACTION = 0.001

  // initialize Spark and sample input data
  val sc = new SparkContext(new SparkConf().setAppName("PreferenceSimilarityTrustFactor").setMaster("local[2]"))
  val ratingAllLines = sc.textFile("data/rating.txt")
  val userRatingAllLines = sc.textFile("data/user_rating.txt")
  val mcAllLines = sc.textFile("data/mc.txt").filter(_.split("\t").length > 2)
  val ratingLines = ratingAllLines.sample(withReplacement = false, RATING_FILE_SAMPLING_FRACTION)
  val userLines = userRatingAllLines.sample(withReplacement = false, MC_FILE_SAMPLING_FRACTION)
  val mcLines = mcAllLines.sample(withReplacement = false, USER_RATING_FILE_SAMPLING_FRACTION)

  def main(args: Array[String]): Unit = {
    // println(PreferenceSimilarityTrustFactor.trustValueBasedOnSameArticles(ratingLines).collect().mkString("\n"))
    // PreferenceSimilarityTrustFactor.trustValueBasedOnSimilarRatings(ratingLines).saveAsTextFile("trustValueBasedOnSimilarRatings")
    PreferenceSimilarityTrustFactor.trustValueBasedOnSameSubjects(ratingLines, mcLines)
  }
}
