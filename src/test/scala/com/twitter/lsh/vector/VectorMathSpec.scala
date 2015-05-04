package com.twitter.lsh.vector

import org.specs.Specification
import org.specs.mock.Mockito

class VectorMathSpec extends Specification with Mockito {
  "Vector Math" should {
    "handle vectors correctly" in {
      VectorMath.dot(Array(1.0), Array(1.0)) mustEqual 1.0
    }
  }
}
