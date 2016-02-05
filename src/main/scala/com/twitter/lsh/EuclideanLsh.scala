package com.twitter.lsh

import com.twitter.lsh.hashing.EuclideanHashFamily
import com.twitter.lsh.stores.{HashTableManagerMemory, VectorStoreMemory}
import com.twitter.lsh.vector.{VectorMath, LshVector}

case class IndexedVector(id:Long, vector:LshVector)

/**
  * LSH Builder that uses Euclidean Distance on Regular Vectors
  * This is the best starting place when in doubt
  */
object EuclideanLsh {
  def build(lshParams: LshParams, vectors: Array[IndexedVector]) = {
    val lsh = apply(lshParams)
    vectors.foreach{vec => lsh.addVector(Set(vec.id), vec.vector)}
    lsh
  }

  def apply(lshParams:LshParams) = {
    implicit val monoid = LshVector.lshVectorMonoid

    val hashFamily = new EuclideanHashFamily(lshParams.radius, lshParams.dimensions)
    val vectorStore = VectorStoreMemory[Long, LshVector]
    val hashTableManager = new HashTableManagerMemory[Long, LshVector](hashFamily, lshParams.hashTables, lshParams.hashFunctions)
    new Lsh[Long, LshVector](hashFamily, vectorStore, hashTableManager, VectorMath.normalize)
  }
}
