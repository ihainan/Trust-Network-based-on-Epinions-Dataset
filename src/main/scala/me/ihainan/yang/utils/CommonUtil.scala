package me.ihainan.yang.utils

import org.apache.spark.rdd.RDD

/**
  * Common utils
  */
object CommonUtil {
  def printRDD(rdd: RDD[_]) = {
    println(rdd.collect.mkString("\n"))
  }

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }
}
