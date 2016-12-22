package me.ihainan.yang.utils

import org.apache.spark.rdd.RDD

/**
  * Created by ihainan on 12/22/16.
  */
object SampleDataUtil {

  def sampleData(ratingInputData: RDD[String],

                 mcInputData: RDD[String],
                 userRatingInputData: RDD[String]) = {
    // 随机选取 20000 条信任数据
    val userRatingsRDD = userRatingInputData.map(line => (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2)))
    val sampledSampleRatingsRDD = userRatingsRDD.sample(withReplacement = false, 200.0 / userRatingsRDD.count)
    CommonUtil.printRDD(sampledSampleRatingsRDD)
  }
}
