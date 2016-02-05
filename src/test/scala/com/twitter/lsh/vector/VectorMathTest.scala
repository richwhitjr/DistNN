package com.twitter.lsh.vector

import org.scalactic.TolerantNumerics
import org.scalatest.Matchers
import org.scalatest.WordSpec

class VectorMathTest extends WordSpec with Matchers {
  import VectorMath._

  implicit val custom = TolerantNumerics.tolerantDoubleEquality(0.01)

  "Vector Math" should {
    "compute dot product" in {
      assert(vectorDot(LshVector(Array(1.0)), LshVector(Array(1.0))) == 1.0)
    }

    "divide" in {
      val vector = LshVector(Array(1.0, 1.0))
      assert(divide(vector, 2.0).vector === Array(0.5, 0.5))
    }

    "multiply" in {
      val vector = LshVector(Array(1.0, 1.0))
      assert(multiply(vector, 2.0).vector === Array(2.0, 2.0))
    }

    "magnitude" in {
      val vector = LshVector(Array(1.0, 1.0))
      assert(magnitude(vector) === 1.41)
    }

    "normalize" in {
      val vector = LshVector(Array(3.0, 2.0, 1.0))
      val normed = normalize(vector).vector

      assert(normed(0) === 0.801)
      assert(normed(1) === 0.534)
      assert(normed(2) === 0.267)
    }

    "sum" in {
      val vector = LshVector(Array(3.0, 2.0, 1.0))
      val vector2 = LshVector(Array(5.0, 5.0, 5.0))

      assert(vectorSum(vector, vector2).vector === LshVector(Array(8.0, 7.0, 6.0)).vector)
    }

    "multiply vectors" in {
      val vector = LshVector(Array(3.0, 2.0, 1.0))
      val vector2 = LshVector(Array(5.0, 5.0, 5.0))

      assert(vectorMultiply(vector, vector2).vector === LshVector(Array(15.0, 10.0, 5.0)).vector)
    }

    "divide vectors" in {
      val vector = LshVector(Array(3.0, 2.0, 1.0))
      val vector2 = LshVector(Array(0.5, 1.0, 1.0))

      assert(vectorDivide(vector, vector2).vector === LshVector(Array(6.0, 2.0, 1.0)).vector)
    }

    "divide vectors with zero" in {
      val vector = LshVector(Array(3.0, 2.0, 1.0))
      val vector2 = LshVector(Array(0.5, 0.0, 1.0))

      assert(vectorDivide(vector, vector2).vector === LshVector(Array(6.0, 0.0, 1.0)).vector)
    }
  }
}
