package me.ihainan.yang.trust

import org.apache.spark.rdd.RDD

/**
  * An application used to calculate the trust value
  */
object FamiliarityTrustFactor {
  val ENABLE_DEBUG = false

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
    * calculate trust value based on users' familiarity value
    *
    * @param inputData input user_rating data
    * @return trust value RDD
    */
  def trustValueBasedOnFamiliarityValue(inputData: RDD[String]): RDD[((String, String), Double)] = {
    val trustDataRDD = inputData.map(parseVectorForUserAndTrustValue) // RDD[User, User]
      .groupByKey() // RDD[User, (User, User)]

    // RDD[(User, User), ([User], [User])]
    val pairTrust = trustDataRDD.cartesian(trustDataRDD) // RDD[(User, [User]), (User, [User])]
      .map(pair => ((pair._1._1, pair._2._1), (pair._1._2, pair._2._2))) // RDD[(User, User), ([User], [User])]
      .filter(pair => pair._1._1 != pair._1._2) // RDD[(User, User), ([User], [User])]

    // RDD[(User, User), Double]
    val userFamiliarity = pairTrust.map(usersAndNeighbours =>
      calculateFamiliarityValue(usersAndNeighbours._1, usersAndNeighbours._2)) // RDD[(User, User), Double]

    userFamiliarity
  }
}
