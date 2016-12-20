package me.ihainan.yang.trust

import org.scalatest.FunSuite

/**
  * Created by ihainan on 12/19/16.
  */
class ReputationTrustFactor$Test extends FunSuite {

  test("testCalculateReputationValue-1") {
    val user = "userA"
    val neighboursAndTrustValue = Array(("UserB", 1), ("UserC", -1), ("UserD", 1), ("UserE", 1))
    val reputationValue = ReputationTrustFactor.calculateReputationValue(user, neighboursAndTrustValue)
    assert(reputationValue._2 == 0.5)
  }

  test("testCalculateTrustValue-1") {
    val users = ("userA", "userB")
    val reputationValue = (0.5, -0.5)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 0)
  }

  test("testCalculateTrustValue-2") {
    val users = ("userA", "userB")
    val reputationValue = (0.6, 1.0)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 0.6)
  }

  test("testCalculateTrustValue-3") {
    val users = ("userA", "userB")
    val reputationValue = (0.4, 0.5)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 0.2)
  }

  test("testCalculateTrustValue-4") {
    val users = ("userA", "userB")
    val reputationValue = (1.0, 0.6)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 0.6)
  }

  test("testCalculateTrustValue-5") {
    val users = ("userA", "userB")
    val reputationValue = (0.6, 1.0)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 0.6)
  }
  test("testCalculateTrustValue-6") {
    val users = ("userA", "userB")
    val reputationValue = (0.4, 0.8)
    val trustValue = ReputationTrustFactor.calculateTrustValue(users, reputationValue)
    assert(trustValue._2 == 2.0 / 15 * Math.exp(-(reputationValue._1 - reputationValue._2)))
  }
}
