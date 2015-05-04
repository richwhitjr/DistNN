package com.twitter.lsh

import com.twitter.lsh.vector._
import com.twitter.util.Await
import org.specs._
import org.specs.mock.Mockito

object LshSetup {
  val vectors = Array(
    IndexedVector(1L, LshVector(Array(1.0, 2.0, 3.0, 4.0))),
    IndexedVector(2L, LshVector(Array(3.0, 2.0, 1.0, 9.0))),
    IndexedVector(3L, LshVector(Array(19.0, 22.0, 13.0, 13.0)))
  )

  val lsh = LshEuclideanDoubleVector.withVectors(LshParams(1, 5, 1.0, 4), vectors)
}

class LshSpec extends Specification with Mockito {
  import com.twitter.lsh.LshSetup._

  "Lsh Memory" should {
    val topN = 10

    val firstVector = vectors(0)
    val lshResults = Await.result(lsh.query(firstVector.vector))
    val scoredResults = lsh.scoreQuery(firstVector.vector, lshResults._2, topN)

    "match results" in {
      scoredResults.size mustEqual 1
    }
  }
}

