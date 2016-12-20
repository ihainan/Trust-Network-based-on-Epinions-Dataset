package me.ihainan.yang.trust

/**
  * PreferenceSimilarityTrustFactor Unit Test
  */
class PreferenceSimilarityTrustFactor$Test extends org.scalatest.FunSuite {
  test("calculateRatingsSimilarity-1") {
    val userPair = ("UserA", "UserB")
    val userARating = Array(("Article1", 5), ("Article2", 3), ("Article3", 4))
    val userBRating = Array(("Article1", 5), ("Article3", 2))
    val (_, similarity) = PreferenceSimilarityTrustFactor.calculateRatingsSimilarity(userPair, (userARating, userBRating))
    assert(similarity == 0.5)
  }

  test("calculateRatingsSimilarity-2") {
    val userPair = ("UserA", "UserB")
    val userARating = Array(("Article1", 5), ("Article2", 3), ("Article3", 4))
    val userBRating = Array(("Article6", 5))
    val (_, similarity) = PreferenceSimilarityTrustFactor.calculateRatingsSimilarity(userPair, (userARating, userBRating))
    assert(similarity == 0)
  }

  test("calculateItemsSimilarity-2") {
    val userPair = ("UserA", "UserB")
    val userAItems = Array("Item1", "Item2", "Item3")
    val userBItems = Array("Item4", "Item1")
    val (_, similarity) = PreferenceSimilarityTrustFactor.calculateItemsSimilarity(userPair, (userAItems, userBItems))
    assert(similarity == 0.25)
  }
}
