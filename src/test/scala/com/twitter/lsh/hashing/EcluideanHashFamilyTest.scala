package com.twitter.lsh.hashing

import com.twitter.lsh.vector.{VectorMath, LshVector}
import org.scalactic.TolerantNumerics
import org.scalatest.{Matchers, WordSpec}

class EcluideanHashFamilyTest extends WordSpec with Matchers {
  import VectorMath._
  implicit val custom = TolerantNumerics.tolerantDoubleEquality(0.001)

  "Ecluidean Hash Family" should {
    val family = new EuclideanHashFamily(1.0, 1)

    "hash correctly" in {
      val hasher = family.createHasher(1, 1)

      assert(hasher.hash(Array(1.0)) == 0)
      assert(hasher.hash(Array(5.0)) == 1)
      assert(hasher.hash(Array(25.0)) == 4)
    }

    "compute diststance" in {
      val vector1 = normalize(LshVector(Array(0.00632,18,2.31,0,0.538,6.575,65.2,4.09,1,296,15.3,396.9,4.98,24)))
      val vector2 = normalize(LshVector(Array(0.02731,0,7.07,0,0.469,6.421,78.9,4.9671,2,242,17.8,396.9,9.14,21.6)))

      assert(family.score(vector1, vector2) === 0.106)
    }
  }
}
