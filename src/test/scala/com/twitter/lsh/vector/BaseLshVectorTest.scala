package com.twitter.lsh.vector

import org.scalatest.{Matchers, WordSpec}

class BaseLshVectorTest extends WordSpec with Matchers {
  "Base Lsh Vector monoid should some arrays" should {
    val monoid = new SummingArrayMonoid[Double]
    val result = monoid.plus(Array(1.0, 2.0), Array(2.0, 4.0))

    "sum correctly" in {
      assert(result === Array(3.0, 6.0))
    }
  }
}
