package me.ihainan.yang.utils

import org.apache.spark.rdd.RDD

/**
  * Common utils
  */
object CommonUtil {
  def printRDD(rdd: RDD[_]) = {
    println(rdd.collect.mkString("\n"))
  }


  def printPairPaths(allUserPairPaths: RDD[((String, String), Iterable[Array[String]])]): Unit = {
    allUserPairPaths.foreach(pair => {
      print(pair._1 + " => ")
      for (path <- pair._2) print("[" + path.mkString(", ") + "] ")
      println()
    })
  }

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }
}
