package me.ihainan.yang.trust

import org.apache.spark.rdd.RDD

/**
  * An application used to calculate the reputation value of every two users
  */
object ReputationTrustFactor {

  /**
    * Parse input line, get author and subject
    *
    * @param line input row
    * @return RDD[Author, Subject]
    */
  private def parseVectorForUserAndTrustValue(line: String): (String, (String, Int)) = {
    val userAndTrustValueData = line.split("\t")
    (userAndTrustValueData(1), (userAndTrustValueData(0), userAndTrustValueData(2).toInt))
  }

  /**
    * Calculate user's reputation value
    *
    * @param user                    The ID of user
    * @param neighboursAndTrustValue User's neighbour and corresponding trust value (1 or -1)
    * @return user's reputation value
    */
  def calculateReputationValue(user: String, neighboursAndTrustValue: Iterable[(String, Int)]): (String, Double) = {
    val numberOfNeighbours = neighboursAndTrustValue.size
    var reputationSum: Double = 0.0f
    for (userAndTrustValue <- neighboursAndTrustValue) reputationSum = reputationSum + userAndTrustValue._2
    val reputationValue = if (numberOfNeighbours == 0) 0.0f else reputationSum * 1.0 / numberOfNeighbours
    (user, reputationValue)
  }

  /**
    * Calculate the trust value based on users' reputation
    *
    * @param users       The ID of user A to B
    * @param reputations All the reputation data of user A and B
    * @return the trust value of user A to B
    */
  def calculateTrustValue(users: (String, String), reputations: (Double, Double)): ((String, String), Double) = {
    val (reputationA, reputationB) = reputations
    val trustValue = if (reputationB < 0.0f) 0.0f.toDouble
    else if (reputationA >= 0 && reputationB >= 0 && reputationA <= 0.5 && reputationB <= 0.5
      || reputationA > 0.5 && reputationB > 0.5 && reputationA <= 1 && reputationB <= 1) {
      reputationA * reputationB
    } else {
      2.0 / 15 * Math.exp(-reputationA + reputationB)
    }

    (users, trustValue)
  }

  /**
    * calculate trust value based on user's reputation value
    *
    * @param inputData input user_rating data
    * @return trust value RDD
    */
  def trustValueBasedOnReputation(inputData: RDD[String]): RDD[((String, String), Double)] = {
    // RDD[User, [(User, TrustValue)]]
    val trustDataRDD = inputData.map(parseVectorForUserAndTrustValue).groupByKey()

    // RDD[User, Reputation]
    val reputationRDD = trustDataRDD.map(pair => calculateReputationValue(pair._1, pair._2))

    // RDD[(User, User), (Reputation, Reputation)]
    val pairRDD = reputationRDD.cartesian(reputationRDD) // RDD[(User, Reputation), (User, Reputation)]
      .map(pair => ((pair._1._1, pair._2._1), (pair._1._2, pair._2._2))) // RDD[(User, User), (Reputation, Reputation)]
      .filter(pair => pair._1._1 != pair._1._2) // RDD[(User, User), (Reputation, Reputation)]

    // RDD[(User, User), TrustValue]
    pairRDD.map(pair => calculateTrustValue(pair._1, pair._2)).filter(_._2 != 0.0f)
  }
}
