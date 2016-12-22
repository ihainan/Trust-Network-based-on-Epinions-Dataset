package me.ihainan.yang.trust

import me.ihainan.yang.utils.CommonUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * An application used to calculate the trust value
  */
object FamiliarityTrustFactor {
  val ENABLE_DEBUG = false
  val MAX_PATH = 5

  /**
    * Parse input line, get user and his trusting friend
    *
    * @param line input row
    * @return RDD[User, User]
    */
  private def parseVectorForUserAndTrustValue(line: String): (String, String) = {
    val userAndTrustValueData = line.split("\t")
    (userAndTrustValueData(0), userAndTrustValueData(1))
  }

  /**
    * Calculate familiarity value of two users
    *
    * @param users      The ID of user A to B
    * @param neighbours User's neighbours
    * @return familiarity value from A to B
    */
  def calculateFamiliarityValue(users: (String, String), neighbours: (Iterable[String], Iterable[String])): ((String, String), Double) = {
    val (userANeighbours, userBNeighbours) = (neighbours._1.toSet, neighbours._2.toSet)
    val allNeighbours = userANeighbours | userBNeighbours
    val commonNeighbours = userANeighbours & userBNeighbours
    val familiarity = if (commonNeighbours.isEmpty) 0.0f else commonNeighbours.size * 1.0 / allNeighbours.size

    if (ENABLE_DEBUG) {
      println(s"userA ${users._1}: " + userANeighbours.mkString(", "))
      println(s"userB ${users._2}: " + userBNeighbours.mkString(", "))
      println("All: " + allNeighbours.mkString(", "))
      println("Common: " + commonNeighbours.mkString(", "))
      println("Familiarity: " + familiarity + "\n")
    }

    (users, familiarity)
  }

  /**
    * Calculate all the paths between users
    *
    * @param trustEdges the edges of trust graph (A trust B )
    * @return all the paths between users
    */
  def calculateAllPaths(trustEdges: RDD[(String, String)]): RDD[((String, String), Iterable[Array[String]])] = {
    var lastStepPaths = trustEdges.map(edge => (edge._2, Array(edge._1)))
    var allPaths: RDD[(String, Array[String])] = lastStepPaths
    for (step <- Range(1, MAX_PATH)) {
      if (lastStepPaths.count() != 0) {
        lastStepPaths = lastStepPaths.join(trustEdges).filter(pair => {
          val newVertice = pair._2._2
          val currentEndVertice = pair._1
          val currentPath = pair._2._1.toList
          newVertice != currentEndVertice && !currentPath.contains(newVertice)
        }).map(pair => (pair._2._2, pair._2._1 :+ pair._1))
        lastStepPaths.cache()
        allPaths = allPaths.union(lastStepPaths)
      }
    }

    allPaths.map(pair => ((pair._2(0), pair._1), pair._2 :+ pair._1)).groupByKey()
  }

  /**
    * Calculate the indirect trust value of each two users
    *
    * @param users              The ID of user A to B
    * @param paths              All the paths between A and B
    * @param userFamiliarityMap All the familiarity value of each two users
    * @return indirect trust value of A and B
    */
  def calculateIndirectTrustValue(users: (String, String), paths: Iterable[Array[String]],
                                  userFamiliarityMap: Map[(String, String), Double]): ((String, String), Double) = {
    var minValue = Double.MaxValue
    for (path <- paths) {
      var trustValue: Double = 1.0f
      for (Array(first, next) <- path.sliding(2))
        trustValue = trustValue * userFamiliarityMap((first.toString, next.toString))
      if (ENABLE_DEBUG) {
        println("PATH: " + path.mkString(", ") + ", trustValue: " + trustValue)
      }
      minValue = minValue.min(trustValue)
    }

    (users, minValue)
  }

  /**
    * calculate trust value based on users' familiarity value
    *
    * @param inputData input user_rating data
    * @return trust value RDD
    */
  def trustValueBasedOnFamiliarityValue(inputData: RDD[String]): RDD[((String, String), Double)] = {
    // RDD[User, [User]]
    val neighboursDataRDD = inputData.map(parseVectorForUserAndTrustValue) // RDD[User, User]
      .groupByKey() // RDD[User, (User, User)]

    // RDD[(User, User), ([User], [User])]
    val pairTrust = neighboursDataRDD.cartesian(neighboursDataRDD) // RDD[(User, [User]), (User, [User])]
      .map(pair => ((pair._1._1, pair._2._1), (pair._1._2, pair._2._2))) // RDD[(User, User), ([User], [User])]
      .filter(pair => pair._1._1 != pair._1._2) // RDD[(User, User), ([User], [User])]

    // RDD[(User, User), Double]
    val userFamiliarity = pairTrust.map(usersAndNeighbours =>
      calculateFamiliarityValue(usersAndNeighbours._1, usersAndNeighbours._2)) // RDD[(User, User), Double]

    // convert RDD[(User, User), Double] to Map
    val userFamiliarityMap = userFamiliarity.filter(_._2 != 0.0).collect().toMap
    SparkContext.getOrCreate().broadcast(userFamiliarityMap)

    // RDD[User, (User, User)]
    val trustDataRDD = inputData.filter(_.split("\t")(2).equals("1")).map(parseVectorForUserAndTrustValue) // RDD[User, User]

    val rdd1 = inputData.map(parseVectorForUserAndTrustValue)
    val rdd2 = trustDataRDD
    CommonUtil.printRDD(rdd2.subtract(rdd1))

    // RDD[(User, User), [(V1, V2, V3, ..)]]
    val trustPathRDD = calculateAllPaths(trustDataRDD)

    // RDD[(User, User), Double]
    val indirectTrustRDD = trustPathRDD.map(pair => calculateIndirectTrustValue(pair._1, pair._2, userFamiliarityMap))

    indirectTrustRDD
  }
}
