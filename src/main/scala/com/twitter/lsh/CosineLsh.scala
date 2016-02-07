package com.twitter.lsh

import com.twitter.lsh.hashing.CosineHashFamily
import com.twitter.lsh.stores.{HashTableManagerMemory, VectorStoreMemory}
import com.twitter.lsh.vector.{IndexedVector, VectorMath, LshVector}

/**
  * Simple LSH Builder for Cosine Distance
  */
object CosineLsh {
  def build(lshParams: LshParams, vectors: Array[IndexedVector[LshVector]]) = {
    val lsh = apply(lshParams)
    vectors.foreach{vec => lsh.addVector(Set(vec.id), vec.vector)}
    lsh
  }

  def apply(lshParams:LshParams) = {
    implicit val monoid = LshVector.lshVectorMonoid

    val hashFamily = new CosineHashFamily(lshParams.dimensions)
    val vectorStore = VectorStoreMemory[Long, LshVector]
    val hashTableManager = new HashTableManagerMemory[Long, LshVector](hashFamily, lshParams.hashTables, lshParams.hashFunctions)

    new Lsh[Long, LshVector](hashFamily, vectorStore, hashTableManager, VectorMath.normalize)
  }
}
