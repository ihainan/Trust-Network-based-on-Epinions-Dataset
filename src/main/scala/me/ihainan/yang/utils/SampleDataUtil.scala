package me.ihainan.yang.utils

import org.apache.spark.rdd.RDD

/**
  * Created by ihainan on 12/22/16.
  */
object SampleDataUtil {
  val NUMBER_OF_SUBJECTS = 500

  def sampleData(ratingInputData: RDD[String],
                 mcInputData: RDD[String],
                 userRatingInputData: RDD[String]) = {
    // 随机选取 1000 个商品（mc.txt 删除掉其他商品）
    val subjectsRDD = mcInputData.map(_.split("\t")(2)).distinct()
    val sampledSubjectsRDD = subjectsRDD.sample(withReplacement = false, NUMBER_OF_SUBJECTS * 1.0 / subjectsRDD.count())
    mcInputData.map(line => (line.split("\t")(2), line)).join(sampledSubjectsRDD.map(subject => (subject, subject))).map(_._2._1).saveAsTextFile("mc_sample.txt")

    // 评论过该商品的用户
    val mcUsersRDD = mcInputData.map(line => (line.split("\t")(2), line.split("\t")(1))).join(sampledSubjectsRDD.map(subject => (subject, subject))).map(_._2._1).distinct()

    // 该商品对应的评论（rating.txt 删除掉用户没有评价过的数据） -> 评价过评论的用户
    val contentIDsRDD = mcInputData.map(line => (line.split("\t")(2), line.split("\t")(0))).join(sampledSubjectsRDD.map(subject => (subject, subject))).map(_._2._1).distinct()
    val ratingUsersRDD = ratingInputData.map(line => (line.split("\t")(0), line.split("\t")(1))).join(contentIDsRDD.map(contentID => (contentID, contentID))).map(_._2._1).distinct()
    ratingInputData.map(line => (line.split("\t")(0), line)).join(contentIDsRDD.map(contentID => (contentID, contentID))).map(_._2._1).saveAsTextFile("rating_sample.txt")

    // 获得最终用户列表
    val allUsers = ratingUsersRDD.union(mcUsersRDD)
    allUsers.cache()
    val userPairsRDD = allUsers.cartesian(allUsers).filter(pair => pair._1 != pair._2).distinct()
    userRatingInputData.map(line => ((line.split("\t")(0), line.split("\t")(1)), line)).join(userPairsRDD.map(pair => (pair, pair))).map(_._2._1).saveAsTextFile("user_rating_sample.txt")
    // println(allUsers.count())
    // println(allUsers.first())

    // 获得最终用户列表中，用户相互评价的数据（user_rating.txt 删除非列表中用户的数据）
  }
}
