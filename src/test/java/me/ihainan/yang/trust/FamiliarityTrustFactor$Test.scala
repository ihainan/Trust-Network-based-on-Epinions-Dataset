package me.ihainan.yang.trust

import org.scalatest.FunSuite

/**
  * Created by ihainan on 12/19/16.
  */
class FamiliarityTrustFactor$Test extends FunSuite {

  test("testCalculateFamiliarityValue") {
    val users = ("userA", "userB")
    val userANeighbours = Array("userC", "userD", "userE")
    val userBNeighbours = Array("userD", "userF")
    val familiarityValue = FamiliarityTrustFactor.calculateFamiliarityValue(users, (userANeighbours, userBNeighbours))
    assert(familiarityValue._2 == 0.25f)
  }

  test("testCalculateFamiliarityValue-2") {
    val users = ("userA", "userB")
    val userANeighbours = Array("userC", "userD", "userE")
    val userBNeighbours = Array("userF", "userG")
    val familiarityValue = FamiliarityTrustFactor.calculateFamiliarityValue(users, (userANeighbours, userBNeighbours))
    assert(familiarityValue._2 == 0.0f)
  }
}
