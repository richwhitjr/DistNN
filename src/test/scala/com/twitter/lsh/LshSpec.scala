package com.twitter.lsh

import com.twitter.lsh.hashing.EuclideanHashFamily
import com.twitter.lsh.stores._
import com.twitter.lsh.vector._
import com.twitter.util.Await
import org.specs._
import org.specs.mock.Mockito

/**
 * Created by IntelliJ IDEA.
 * User: rwhitcomb
 * Date: 8/7/14
 * Time: 9:25 AM
 *
 */

object LshSetup {
  val vectors = Array(
    (1L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0))),
    (2L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0))),
    (3L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0))),
    (4L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0))),
    (5L, DoubleLshVector(Array(1.0, 2.0, 3.0, 4.0)))
  )
  
  val lshParams = LshParams(8, 24, 8.0, 4)

  implicit val monoid = DoubleLshVector.doubleLshVectorMonoid

  val hashFamily = new EuclideanHashFamily(lshParams.radius, lshParams.dimensions)
  val vectorStore = VectorStoreMemory[Long, DoubleLshVector]
  val hashTableManager = new HashTableManagerMemory[Long](hashFamily, lshParams.hashTables, lshParams.hashFunctions)
  val lsh = new Lsh[Long, DoubleLshVector, DoubleLshVector](hashFamily, VectorMath.normalize, vectorStore, hashTableManager)
  vectors.map{case((id, vec)) => lsh.update(Set(id), vec)}
}

class LshSpec extends Specification with Mockito {
  import LshSetup._

  "Lsh Memory" should {
    val topN = 10

    val firstVector = vectors(0)
    val lshResults = Await.result(lsh.queryNormalizedVector(firstVector._2))
    val scoredResults = lsh.scoreQuery(firstVector._2, lshResults._2, topN)

    "match results" in {
    }
  }
}

