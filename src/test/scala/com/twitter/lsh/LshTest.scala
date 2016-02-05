package com.twitter.lsh

import com.twitter.lsh.vector._
import com.twitter.util.Await
import org.scalatest.{Matchers, WordSpec}

object LshSetup {
  val vectors = Array(
    IndexedVector(1L, LshVector(Array(1.0, 2.0, 3.0, 4.0))),
    IndexedVector(2L, LshVector(Array(3.0, 2.0, 1.0, 9.0))),
    IndexedVector(3L, LshVector(Array(19.0, 22.0, 13.0, 13.0)))
  )

  val lsh = EuclideanLsh.build(LshParams(1, 5, 1.0, 4), vectors)
}

class EuclideanLshTest extends WordSpec with Matchers {
  import com.twitter.lsh.LshSetup._

  "Lsh Memory" should {
    val topN = 10

    "match results" in {
      val firstVector = vectors.head
      val scoredResults = Await.result(lsh.queryVector(firstVector.vector, topN))

      assert(scoredResults.size == 1)
      assert(scoredResults.head.key == 1L)
    }

    "match results by key" in {
      val scoredResults = Await.result(lsh.queryKey(1L, topN))

      assert(scoredResults.size == 1)
      assert(scoredResults.head.key == 1L)
    }

    "get candidates from lsh" in {
      val firstVector = vectors.head
      val (_, lshResults) = Await.result(lsh.queryLshCandidates(firstVector.vector))

      assert(lshResults.size == 1)
      assert(lshResults.keySet.contains(1L))
    }
  }
}

