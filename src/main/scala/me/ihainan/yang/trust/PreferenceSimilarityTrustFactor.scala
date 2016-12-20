package me.ihainan.yang.trust

import org.apache.spark.rdd.RDD

/**
  * An application used to calculate the preference similarity of every two users
  */
object PreferenceSimilarityTrustFactor {
  val ENABLE_DEBUG = false

  /**
    * Parse input line, get user and the corresponding rating on article
    *
    * @param line input row
    * @return RDD[(User, (Article, Rating))]
    */
  private def parseVectorForUserArticleAndRating(line: String): (String, (String, Int)) = {
    val ratingData = line.split("\t")
    val rating = ratingData(2).toInt
    (ratingData(1), (ratingData(0), if (rating > 5) 5 else rating))
  }

  /**
    * Calculate the similarity based on users' ratings
    *
    * @param users      The ID of user A and B
    * @param ratingData All the rating data of user A and B
    * @return the similarity value of user A and B
    */
  def calculateRatingsSimilarity(users: (String, String),
                                 ratingData: (Iterable[(String, Int)], Iterable[(String, Int)])) = {
    // get the common articles that both user A and B have rated
    val (userAObject, userBObject) = (ratingData._1.map(_._1).toSet, ratingData._2.map(_._1).toSet)
    val commonArticles = userAObject & userBObject

    // get the average values for ALL articles user A and B have rated
    val (userARating, userBRating) = (ratingData._1.map(_._2).toList, ratingData._2.map(_._2).toList)
    val averageA = userARating.sum * 1.0 / userARating.length
    val averageB = userBRating.sum * 1.0 / userBRating.length

    // Calculate similarity value
    val (userARatingMap, userBRatingMap) = (ratingData._1.toMap, ratingData._2.toMap)
    if (commonArticles.nonEmpty) {
      var sumAB: Double = 0.0f
      var sumA: Double = 0.0f
      var sumB: Double = 0.0f
      for (preference <- commonArticles) sumAB = sumAB +
        ((userARatingMap(preference) - averageA) * (userBRatingMap(preference) - averageB))
      for (rating <- userARating) sumA = sumA + Math.pow(rating - averageA, 2)
      for (rating <- userBRating) sumB = sumB + Math.pow(rating - averageB, 2)
      val similarityValue = (users, if (sumA == 0 || sumB == 0) 1 else sumAB / Math.sqrt(sumA * sumB))

      if (ENABLE_DEBUG) {
        println(s"userA ${users._1}: ${userARatingMap.mkString(", ")}")
        println(s"userB ${users._2}: ${userBRatingMap.mkString(", ")}")
        println(s"Common: ${commonArticles.mkString(", ")}")
        println(s"averageA = $averageA, averageB = $averageB, sumAB = $sumAB, sumA = $sumA, sumB = $sumB")
        println(s"SimilarityValue: $similarityValue")
      }

      similarityValue
    } else {
      (users, 0.toDouble) // have nothing similar
    }
  }

  /**
    * Calculate trust value based on the ratings of different users
    *
    * @param inputData input data
    * @return trust value RDD
    */
  def trustValueBasedOnSameArticles(inputData: RDD[String]): RDD[((String, String), Double)] = {
    // RDD[(USER, [Article, Rating])]
    val userRatingRDD = inputData.map(parseVectorForUserArticleAndRating).groupByKey()

    // RDD[(USER, USER), ([(Article, Rating)], [(Article, Rating)])]
    val pairRDD = userRatingRDD.cartesian(userRatingRDD)
      .map(pair => ((pair._1._1, pair._2._1), (pair._1._2, pair._2._2)))
      .filter(pair => pair._1._1 != pair._1._2) // remove same user

    // RDD[(USER, USER), TrustValue]
    pairRDD.map(pair => calculateRatingsSimilarity(pair._1, pair._2))
  }

  /**
    * Parse input line, get article object and user member
    *
    * @param line input row
    * @return RDD[Article, Member]
    */
  private def parseVectorForObjectAndMember(line: String): (String, String) = {
    val objectAndMemberData = line.split("\t")
    (objectAndMemberData(0), objectAndMemberData(1))
  }

  /**
    * Parse input line, get article and subject
    *
    * @param line input row
    * @return RDD[Article, Subject]
    */
  private def parseVectorForObjectAndSubject(line: String): (String, String) = {
    val objectAndSubjectData = line.split("\t")
    if (objectAndSubjectData.length <= 2)
      println(s"line = ${line}")
    (objectAndSubjectData(0), objectAndSubjectData(2))
  }

  /**
    * Parse input line, get author and subject
    *
    * @param line input row
    * @return RDD[Author, Subject]
    */
  private def parseVectorForAuthorAndSubject(line: String): (String, String) = {
    val authorAndSubjectData = line.split("\t")
    (authorAndSubjectData(1), authorAndSubjectData(2))
  }

  /**
    * Calculate the similarity based on users' focusing items
    *
    * @param users    The ID of user A and B
    * @param subjects All the focusing items of user A and B
    * @return the similarity value of user A and B
    */
  def calculateItemsSimilarity(users: (String, String), subjects: (Iterable[String], Iterable[String])) = {
    val (userASubjects, userBSubjects) = (subjects._1.toSet, subjects._2.toSet)
    val allSubjects = userASubjects | userBSubjects
    val commonSubjects = userASubjects & userBSubjects
    val similarity = if (commonSubjects.isEmpty) 0.0f else commonSubjects.size * 1.0 / allSubjects.size

    if (commonSubjects.nonEmpty && ENABLE_DEBUG) {
      println(s"userA ${users._1}: " + userASubjects.mkString(", "))
      println(s"userB ${users._2}: " + userBSubjects.mkString(", "))
      println("All: " + allSubjects.mkString(", "))
      println("Common: " + commonSubjects.mkString(", "))
      println("Similarity: " + similarity + "\n")
    }

    (users, similarity)
  }

  /**
    * Calculate trust value based on the common focusing items of different users
    *
    * @param ratingInputData input rating data
    * @param mcInputData     input article data
    * @return trust value RDD
    */
  def trustValueBasedOnSameSubjects(ratingInputData: RDD[String],
                                    mcInputData: RDD[String]): RDD[((String, String), Double)] = {
    // RDD[Article, Member]
    val objectAndMemberRDD = ratingInputData.map(parseVectorForObjectAndMember)

    // RDD[Article, Subject]
    val objectAndSubjectRDD = mcInputData.map(parseVectorForObjectAndSubject)

    // RDD[Author, Subject]
    val authorAndSubjectRDD = mcInputData.map(parseVectorForAuthorAndSubject)

    // RDD[User, Subject]
    val memberAndSubjectPartRDD = objectAndMemberRDD.join(objectAndSubjectRDD) // RDD[Article, (User, Subject)]
      .map(pair => (pair._2._1, pair._2._2)) // RDD[User, Subject]

    // RDD[User, [Subject]]
    val memberAndSubjectRDD = memberAndSubjectPartRDD.union(authorAndSubjectRDD).distinct() // RDD[User, Subject]
      .groupByKey() // RDD[User, [Subject]]

    // RDD[(User, User), ([Subject], [Subject])]
    val pairRDD = memberAndSubjectRDD.cartesian(memberAndSubjectRDD) // RDD[(User, [Subject]), (User, Subject))]
      .map(pair => ((pair._1._1, pair._2._1), (pair._1._2, pair._2._2))) // RDD[(User, User), ([Subject], [Subject])]
      .filter(pair => pair._1._1 != pair._1._2) // RDD[(User, User), ([Subject], [Subject])]

    // RDD[(User, User), TrustValue]
    pairRDD.map(pair => calculateItemsSimilarity(pair._1, pair._2))
  }
}
