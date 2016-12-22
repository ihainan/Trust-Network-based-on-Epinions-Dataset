package me.ihainan.yang.trust

import me.ihainan.yang.utils.SampleDataUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Trust Network Application
  */
object TrustNetworkApplication {
  // sample fraction of rating.txt file
  val RATING_FILE_SAMPLING_FRACTION = 0.01
  // sample fraction of mc.txt file
  val MC_FILE_SAMPLING_FRACTION = 0.01
  // sample fraction of user_rating.txt file
  val USER_RATING_FILE_SAMPLING_FRACTION = 0.01

  // initialize Spark and sample input data
  val sc = new SparkContext(new SparkConf().setAppName("PreferenceSimilarityTrustFactor").setMaster("local[*]"))

  def main(args: Array[String]): Unit = {
    val ratingAllLines = sc.textFile(args(0)).filter(_.split("\t").length == 8)
    val userRatingAllLines = sc.textFile(args(1)).filter(_.split("\t").length == 4)
    val mcAllLines = sc.textFile(args(2)).filter(_.split("\t").length == 3)
    val ratingLines = ratingAllLines.sample(withReplacement = false, RATING_FILE_SAMPLING_FRACTION)
    val userLines = userRatingAllLines.sample(withReplacement = false, USER_RATING_FILE_SAMPLING_FRACTION)
    val mcLines = mcAllLines.sample(withReplacement = false, MC_FILE_SAMPLING_FRACTION)

    // SampleDataUtil.sampleData(ratingLines, mcLines, userLines)
    PreferenceSimilarityTrustFactor.trustValueBasedOnSameArticles(ratingLines).saveAsTextFile("trustValueBasedOnSameArticles")
    PreferenceSimilarityTrustFactor.trustValueBasedOnSameSubjects(ratingLines, mcLines).saveAsTextFile("trustValueBasedOnSameSubjects")
    // FamiliarityTrustFactor.trustValueBasedOnFamiliarityValue(userLines)
  }
}
