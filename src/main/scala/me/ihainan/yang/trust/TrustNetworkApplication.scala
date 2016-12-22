package me.ihainan.yang.trust

import me.ihainan.yang.utils.SampleDataUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Trust Network Application
  */
object TrustNetworkApplication {
  // sample fraction of rating.txt file
  val RATING_FILE_SAMPLING_FRACTION = 1.0
  // sample fraction of mc.txt file
  val MC_FILE_SAMPLING_FRACTION = 1.0
  // sample fraction of user_rating.txt file
  val USER_RATING_FILE_SAMPLING_FRACTION = 1.0

  // initialize Spark and sample input data
  val sc = new SparkContext(new SparkConf().setAppName("PreferenceSimilarityTrustFactor").setMaster("local[*]"))
  val ratingAllLines = sc.textFile("data/rating.txt")
  val userRatingAllLines = sc.textFile("data/user_rating.txt")
  val mcAllLines = sc.textFile("data/mc.txt").filter(_.split("\t").length > 2)
  val ratingLines = ratingAllLines.sample(withReplacement = false, RATING_FILE_SAMPLING_FRACTION)
  val userLines = userRatingAllLines.sample(withReplacement = false, USER_RATING_FILE_SAMPLING_FRACTION)
  val mcLines = mcAllLines.sample(withReplacement = false, MC_FILE_SAMPLING_FRACTION)

  def main(args: Array[String]): Unit = {
    // SampleDataUtil.sampleData(ratingLines, mcLines, userLines)
    PreferenceSimilarityTrustFactor.trustValueBasedOnSameArticles(ratingLines).saveAsTextFile("trustValueBasedOnSameArticles")
    // PreferenceSimilarityTrustFactor.trustValueBasedOnSimilarRatings(ratingLines).saveAsTextFile("trustValueBasedOnSimilarRatings")
    // PreferenceSimilarityTrustFactor.trustValueBasedOnSameSubjects(ratingLines, mcLines)
    // FamiliarityTrustFactor.trustValueBasedOnFamiliarityValue(userLines)
  }
}
