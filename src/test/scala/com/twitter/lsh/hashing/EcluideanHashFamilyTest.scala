package com.twitter.lsh.hashing

import org.scalatest.{Matchers, WordSpec}

class EcluideanHashFamilyTest extends WordSpec with Matchers {
  "Ecluidean Hash Family" should {
    "hash correctly" in {

      val family = new EuclideanHashFamily(1.0, 1)
      val hasher = family.createHasher(1, 1)

      assert(hasher.hash(Array(1.0)) == 0)
      assert(hasher.hash(Array(5.0)) == 1)
      assert(hasher.hash(Array(25.0)) == 4)
    }
  }
}
