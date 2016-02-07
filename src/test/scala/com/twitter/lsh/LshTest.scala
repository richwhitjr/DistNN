package com.twitter.lsh

import com.twitter.lsh.vector._
import com.twitter.util.Await
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

object LshSetup {
  val vectors = Array(
    IndexedVector(1L, LshVector(Array(1.0, 2.0, 3.0, 4.0))),
    IndexedVector(2L, LshVector(Array(3.0, 2.0, 1.0, 9.0))),
    IndexedVector(3L, LshVector(Array(19.0, 22.0, 13.0, 13.0)))
  )

  val lsh = CosineLsh.build(LshParams(1, 5, 4), vectors)
}

class EuclideanLshTest extends WordSpec with Matchers {
  import com.twitter.lsh.LshSetup._

  "Lsh Memory" should {
    val topN = 10

    "match results" in {
      val firstVector = vectors.head
      val scoredResults = Await.result(lsh.queryVector(firstVector.vector, topN))

      assert(scoredResults.size == 2)
      assert(scoredResults.head.key == 1L)
    }

    "match results by key" in {
      val scoredResults = Await.result(lsh.queryKey(1L, topN))

      assert(scoredResults.size == 2)
      assert(scoredResults.head.key == 1L)
    }

    "get candidates from lsh" in {
      val firstVector = vectors.head
      val (_, lshResults) = Await.result(lsh.queryLshCandidates(firstVector.vector))

      assert(lshResults.size == 2)
      assert(lshResults.keySet.contains(1L))
    }
  }

  "Lsh on Real Data" should {
    val vectors = Source
      .fromFile("src/test/resources/data/boston_housing_prices.csv")
      .getLines()
      .toArray
      .tail
      .zipWithIndex
      .map{case(row, idx) => IndexedVector(idx, LshVector(row.split(",").map(_.toDouble)))}

    val expected = List(
      ScoredResult(1, 0.0),
      ScoredResult(47, 4.898027321099674E-4),
      ScoredResult(49, 8.532084052118583E-4),
      ScoredResult(2, 0.0011394038949388285),
      ScoredResult(87, 0.0011757478066938276),
      ScoredResult(337, 0.0012564211670719194),
      ScoredResult(88, 0.0013214770760161532),
      ScoredResult(85, 0.0013278585432627832),
      ScoredResult(340, 0.0014301127625744314),
      ScoredResult(91, 0.0015501924628366082)
    )

    "find vectors" in {
      val lsh = CosineLsh.build(LshParams(1, 5, 13), vectors)

      val results = Await.result(lsh.queryKey(1, 10))

      assert(results === expected)
    }
  }
}

