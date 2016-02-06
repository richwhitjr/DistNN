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

  "Lsh on Real Data" should {
    val vectors = Source
      .fromFile("src/test/resources/data/boston_housing_prices.csv")
      .getLines()
      .toArray
      .tail
      .zipWithIndex
      .map{case(row, idx) => IndexedVector(idx, LshVector(row.split(",").map(_.toDouble)))}

    val expected = List(
      ScoredResult(1, 1.0),
      ScoredResult(47, 0.9995101972678899),
      ScoredResult(49, 0.9991467915947881),
      ScoredResult(2, 0.9988605961050612),
      ScoredResult(87, 0.9988242521933062),
      ScoredResult(337, 0.9987435788329281),
      ScoredResult(88, 0.9986785229239838),
      ScoredResult(85, 0.9986721414567372),
      ScoredResult(340, 0.9985698872374255),
      ScoredResult(91, 0.9984498075371634)
    )

    "find vectors" in {
      val lsh = EuclideanLsh.build(LshParams(5, 10, 1.0, 13), vectors)

      val results = Await.result(lsh.queryKey(1, 10))

      assert(results === expected)
    }
  }
}

