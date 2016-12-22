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

  test("testCalculateIndirectTrustValue-1") {
    val users = ("A", "D")
    val paths = Iterable(Array("A", "B", "C", "D"), Array("A", "E", "D"))
    var userFamiliarityMap = Map.empty[(String, String), Double]
    userFamiliarityMap += (("A", "B") -> 1.0)
    userFamiliarityMap += (("B", "C") -> 0.5)
    userFamiliarityMap += (("C", "D") -> 0.3)
    userFamiliarityMap += (("A", "E") -> 1.0)
    userFamiliarityMap += (("E", "D") -> 0.2)
    val trustValue = FamiliarityTrustFactor.calculateIndirectTrustValue(users, paths, userFamiliarityMap)._2
    assert(trustValue == 0.15)
  }
}