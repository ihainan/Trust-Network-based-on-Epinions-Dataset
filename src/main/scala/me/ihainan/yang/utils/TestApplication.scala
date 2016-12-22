package me.ihainan.yang.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ihainan on 12/22/16.
  */
object TestApplication {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("PreferenceSimilarityTrustFactor").setMaster("local[*]"))
    
    val mcAllLines = sc.textFile("mc_sample.txt").map(_.split("\t")(0)).map(objectID => (objectID, objectID))
    val ratingAllLines = sc.textFile("rating_sample.txt").map(line => (line.split("\t")(0), line)).join(mcAllLines)
    CommonUtil.printRDD(ratingAllLines)
  }
}
